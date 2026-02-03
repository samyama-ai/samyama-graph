const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const Redis = require('ioredis');
const rateLimit = require('express-rate-limit');

const app = express();
const PORT = process.env.PORT || 8080;

// Connect to Samyama (Redis-compatible protocol)
const samyama = new Redis({
  host: process.env.SAMYAMA_HOST || '127.0.0.1',
  port: process.env.SAMYAMA_PORT || 6379,
  maxRetriesPerRequest: 3,
  retryDelayOnFailover: 100
});

samyama.on('error', (err) => {
  console.error('Samyama connection error:', err.message);
});

samyama.on('connect', () => {
  console.log('Connected to Samyama Graph DB');
});

// Middleware
app.use(helmet());
app.use(morgan('combined'));
app.use(express.json({ limit: '10kb' }));

// CORS - allow samyama.dev and localhost for development
app.use(cors({
  origin: [
    'https://samyama.dev',
    'https://www.samyama.dev',
    'http://localhost:3000',
    'http://localhost:5173',
    'http://localhost:8080'
  ],
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

// Rate limiting: 30 requests per minute per IP
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 30,
  message: {
    error: 'Too many requests. Please try again in a minute.',
    retryAfter: 60
  },
  standardHeaders: true,
  legacyHeaders: false
});

app.use('/api/', limiter);

// Query validation
const BLOCKED_KEYWORDS = [
  'CREATE', 'MERGE', 'DELETE', 'DETACH DELETE',
  'SET', 'REMOVE', 'DROP', 'CALL', 'LOAD CSV',
  'FOREACH'
];

function validateQuery(query) {
  if (!query || typeof query !== 'string') {
    return { valid: false, error: 'Query must be a non-empty string' };
  }
  
  const trimmed = query.trim();
  
  if (trimmed.length > 2000) {
    return { valid: false, error: 'Query too long (max 2000 characters)' };
  }
  
  if (trimmed.length < 10) {
    return { valid: false, error: 'Query too short' };
  }
  
  const upperQuery = trimmed.toUpperCase();
  
  // Block write operations
  for (const blocked of BLOCKED_KEYWORDS) {
    const regex = new RegExp('\\b' + blocked.replace(' ', '\\s+') + '\\b', 'i');
    if (regex.test(upperQuery)) {
      return { valid: false, error: `Operation not allowed: ${blocked}. This is a read-only sandbox.` };
    }
  }
  
  // Must have MATCH clause
  if (!upperQuery.includes('MATCH')) {
    return { valid: false, error: 'Query must contain a MATCH clause' };
  }
  
  // Must have RETURN clause
  if (!upperQuery.includes('RETURN')) {
    return { valid: false, error: 'Query must contain a RETURN clause' };
  }
  
  return { valid: true };
}

// Health check
app.get('/health', async (req, res) => {
  try {
    const pong = await samyama.ping();
    res.json({ status: 'healthy', samyama: pong === 'PONG' ? 'connected' : 'error' });
  } catch (err) {
    res.status(503).json({ status: 'unhealthy', error: err.message });
  }
});

// Execute Cypher query
app.post('/api/query', async (req, res) => {
  const { query, graph = 'sandbox' } = req.body;
  
  // Validate query
  const validation = validateQuery(query);
  if (!validation.valid) {
    return res.status(400).json({ error: validation.error });
  }
  
  try {
    // Execute with timeout (5 seconds)
    const result = await Promise.race([
      samyama.call('GRAPH.QUERY', graph, query),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Query timeout (5s)')), 5000)
      )
    ]);
    
    // Parse result
    const response = {
      success: true,
      data: result,
      query: query.substring(0, 100) + (query.length > 100 ? '...' : '')
    };
    
    res.json(response);
  } catch (err) {
    console.error('Query error:', err.message);
    res.status(500).json({ 
      error: 'Query execution failed', 
      message: err.message 
    });
  }
});

// Get available graphs
app.get('/api/graphs', async (req, res) => {
  try {
    const graphs = await samyama.call('GRAPH.LIST');
    res.json({ graphs: graphs || [] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Sample queries endpoint
app.get('/api/samples', (req, res) => {
  res.json({
    samples: [
      {
        name: 'Find all nodes',
        query: 'MATCH (n) RETURN n LIMIT 10'
      },
      {
        name: 'Find relationships',
        query: 'MATCH (a)-[r]->(b) RETURN a, type(r), b LIMIT 10'
      },
      {
        name: 'Find by label',
        query: 'MATCH (n:Person) RETURN n.name LIMIT 10'
      },
      {
        name: 'Shortest path',
        query: 'MATCH p=shortestPath((a)-[*]-(b)) WHERE a.name = "Alice" AND b.name = "Bob" RETURN p'
      }
    ]
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`API Gateway running on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
