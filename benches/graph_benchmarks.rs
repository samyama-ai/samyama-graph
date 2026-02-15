use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::parser::parse_query;
use samyama::query::executor::QueryExecutor;

/// Benchmark node insertion throughput
fn bench_node_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("node_insertion");

    for size in [100, 1000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut store = GraphStore::new();
                for i in 0..size {
                    let id = store.create_node("Person");
                    if let Some(node) = store.get_node_mut(id) {
                        node.set_property("name", format!("Person{}", i));
                        node.set_property("age", (i % 100) as i64);
                    }
                }
            });
        });
    }
    group.finish();
}

/// Benchmark label scan performance
fn bench_label_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("label_scan");

    for size in [100, 1000, 10_000].iter() {
        // Setup: create nodes
        let mut store = GraphStore::new();
        for i in 0..*size {
            let id = store.create_node("Person");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Person{}", i));
            }
        }
        // Also add some noise nodes
        for i in 0..(*size / 2) {
            let id = store.create_node("Company");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", format!("Company{}", i));
            }
        }

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let nodes = store.get_nodes_by_label(&Label::new("Person"));
                criterion::black_box(nodes.len());
            });
        });
    }
    group.finish();
}

/// Benchmark multi-hop traversal latency
fn bench_traversal(c: &mut Criterion) {
    let mut group = c.benchmark_group("traversal");

    // Create a chain: n0 -> n1 -> n2 -> ... -> n99
    let mut store = GraphStore::new();
    let mut node_ids = Vec::new();
    for i in 0..100 {
        let id = store.create_node("Person");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", format!("Person{}", i));
            node.set_property("depth", i as i64);
        }
        node_ids.push(id);
    }
    for i in 0..99 {
        store.create_edge(node_ids[i], node_ids[i + 1], "KNOWS").unwrap();
    }

    // 1-hop traversal
    group.bench_function("1_hop", |b| {
        b.iter(|| {
            let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name").unwrap();
            let executor = QueryExecutor::new(&store);
            let result = executor.execute(&query).unwrap();
            criterion::black_box(result.records.len());
        });
    });

    // 2-hop traversal
    group.bench_function("2_hop", |b| {
        b.iter(|| {
            let query = parse_query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) RETURN c.name").unwrap();
            let executor = QueryExecutor::new(&store);
            let result = executor.execute(&query).unwrap();
            criterion::black_box(result.records.len());
        });
    });

    group.finish();
}

/// Benchmark WHERE clause filtering speed
fn bench_where_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("where_filter");

    let mut store = GraphStore::new();
    for i in 0..1000 {
        let id = store.create_node("Person");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("name", format!("Person{}", i));
            node.set_property("age", (i % 100) as i64);
            node.set_property("active", i % 2 == 0);
        }
    }

    group.bench_function("equality", |b| {
        b.iter(|| {
            let query = parse_query("MATCH (n:Person) WHERE n.age = 25 RETURN n.name").unwrap();
            let executor = QueryExecutor::new(&store);
            let result = executor.execute(&query).unwrap();
            criterion::black_box(result.records.len());
        });
    });

    group.bench_function("comparison", |b| {
        b.iter(|| {
            let query = parse_query("MATCH (n:Person) WHERE n.age > 50 RETURN n.name").unwrap();
            let executor = QueryExecutor::new(&store);
            let result = executor.execute(&query).unwrap();
            criterion::black_box(result.records.len());
        });
    });

    group.bench_function("compound", |b| {
        b.iter(|| {
            let query = parse_query("MATCH (n:Person) WHERE n.age > 20 AND n.age < 40 RETURN n.name").unwrap();
            let executor = QueryExecutor::new(&store);
            let result = executor.execute(&query).unwrap();
            criterion::black_box(result.records.len());
        });
    });

    group.finish();
}

/// Benchmark Cypher parse time
fn bench_cypher_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("cypher_parse");

    group.bench_function("simple_match", |b| {
        b.iter(|| {
            criterion::black_box(parse_query("MATCH (n:Person) RETURN n").unwrap());
        });
    });

    group.bench_function("match_where_return", |b| {
        b.iter(|| {
            criterion::black_box(parse_query(
                "MATCH (n:Person) WHERE n.age > 30 AND n.name = 'Alice' RETURN n.name, n.age"
            ).unwrap());
        });
    });

    group.bench_function("multi_hop", |b| {
        b.iter(|| {
            criterion::black_box(parse_query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:WORKS_AT]->(c:Company) WHERE a.age > 25 RETURN a.name, b.name, c.name"
            ).unwrap());
        });
    });

    group.bench_function("aggregation", |b| {
        b.iter(|| {
            criterion::black_box(parse_query(
                "MATCH (n:Person) RETURN n.dept, count(n), avg(n.age) ORDER BY count(n) DESC LIMIT 10"
            ).unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_node_insertion,
    bench_label_scan,
    bench_traversal,
    bench_where_filter,
    bench_cypher_parse,
);
criterion_main!(benches);
