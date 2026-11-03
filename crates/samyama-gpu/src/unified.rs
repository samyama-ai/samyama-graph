//! Unified-memory buffers — the seam that lets the CPU and GPU share ONE live copy of
//! the graph with no host<->device ETL (paper 22, contribution C1 / workstream Phase 3B).
//!
//! Two backings:
//!   - [`UnifiedBuffer::Host`]    — a plain `Vec<T>`; works on any hardware; GPU access
//!                                  still requires an explicit upload/copy.
//!   - [`UnifiedBuffer::Managed`] — (cuda feature) one `cuMemAllocManaged` allocation that
//!                                  is addressable by BOTH the CPU and CUDA kernels. No copy.
//!
//! On a discrete GPU (e.g. RTX 4050) managed memory is *software* unified memory: pages
//! migrate over PCIe on first touch (this is paper 22's E3a control). On coherent hardware
//! (GH200 via NVLink-C2C; AMD MI300A) the same allocation is cache-coherent with no
//! migration — the E3b win. **The code path is identical; only the hardware differs.**
//! That equivalence is exactly the paper's thesis, which is why the seam is worth building
//! now and validating on a GH200 later.
//!
//! wgpu has no managed-memory concept, so this seam is CUDA-only by construction.

/// Whether unified-memory allocation is requested. Opt-in via `SAMYAMA_GPU_UM=on`.
///
/// Default off: the coherent win only materialises on GH200/MI300A, and on a discrete GPU
/// software UM can be *slower* than an explicit copy (paper 22 E3a characterises exactly
/// when). Keeping it opt-in also means the default path is unchanged and fully tested.
pub fn um_enabled() -> bool {
    matches!(
        std::env::var("SAMYAMA_GPU_UM").ok().as_deref(),
        Some("on") | Some("1") | Some("true")
    )
}

/// A buffer of `T` readable/writable from the CPU, and — when managed — directly
/// addressable by CUDA kernels without a host<->device copy.
pub enum UnifiedBuffer<T: Copy> {
    /// Host-resident `Vec`. GPU access requires an explicit upload.
    Host(Vec<T>),
    /// CUDA managed (unified) allocation: one copy, CPU- and GPU-addressable.
    #[cfg(feature = "cuda")]
    Managed(ManagedBuffer<T>),
}

impl<T: Copy> UnifiedBuffer<T> {
    /// Wrap an existing host vector (no GPU sharing).
    pub fn host(v: Vec<T>) -> Self {
        UnifiedBuffer::Host(v)
    }

    /// Allocate a buffer from `src`, choosing the backing per [`um_enabled`] and CUDA
    /// availability. Falls back to a host buffer if managed allocation is unavailable,
    /// so callers can use this unconditionally.
    pub fn from_slice(src: &[T]) -> Self {
        #[cfg(feature = "cuda")]
        {
            if um_enabled() {
                match ManagedBuffer::from_slice(src) {
                    Ok(m) => return UnifiedBuffer::Managed(m),
                    Err(e) => {
                        tracing::warn!("managed allocation failed, using host buffer: {}", e)
                    }
                }
            }
        }
        UnifiedBuffer::Host(src.to_vec())
    }

    pub fn as_slice(&self) -> &[T] {
        match self {
            UnifiedBuffer::Host(v) => v.as_slice(),
            #[cfg(feature = "cuda")]
            UnifiedBuffer::Managed(m) => m.as_slice(),
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        match self {
            UnifiedBuffer::Host(v) => v.as_mut_slice(),
            #[cfg(feature = "cuda")]
            UnifiedBuffer::Managed(m) => m.as_mut_slice(),
        }
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True if this buffer is a shared (managed) allocation rather than host-only.
    pub fn is_managed(&self) -> bool {
        #[cfg(feature = "cuda")]
        {
            return matches!(self, UnifiedBuffer::Managed(_));
        }
        #[cfg(not(feature = "cuda"))]
        {
            false
        }
    }

    /// Raw device pointer for CUDA kernels, if this buffer is GPU-addressable without a
    /// copy. `Host` buffers return `None` (they must be uploaded first).
    #[cfg(feature = "cuda")]
    pub fn device_ptr(&self) -> Option<u64> {
        match self {
            UnifiedBuffer::Host(_) => None,
            UnifiedBuffer::Managed(m) => Some(m.device_ptr()),
        }
    }
}

#[cfg(feature = "cuda")]
mod managed {
    use crate::error::GpuError;
    use cudarc::driver::{result, sys};
    use std::marker::PhantomData;

    /// A CUDA managed (unified-memory) allocation. Freed on drop.
    ///
    /// # Invariants
    /// - `ptr` addresses `len * size_of::<T>()` bytes returned by `cuMemAllocManaged`.
    /// - A CUDA context must be current when constructed — ensure
    ///   `CudaGpuContext::try_global()` has returned `Some` on this thread first.
    pub struct ManagedBuffer<T: Copy> {
        ptr: sys::CUdeviceptr,
        len: usize,
        _marker: PhantomData<T>,
    }

    impl<T: Copy> ManagedBuffer<T> {
        /// Allocate `len` (uninitialised) elements in managed memory.
        pub fn new(len: usize) -> Result<Self, GpuError> {
            let bytes = len.saturating_mul(std::mem::size_of::<T>()).max(1);
            // SAFETY: `bytes` matches `len`; the returned pointer is used only within
            // this handle's bounds and freed exactly once in `Drop`.
            let ptr = unsafe {
                result::malloc_managed(bytes, sys::CUmemAttach_flags::CU_MEM_ATTACH_GLOBAL)
                    .map_err(|e| GpuError::DeviceCreation(format!("cuMemAllocManaged: {e}")))?
            };
            Ok(Self { ptr, len, _marker: PhantomData })
        }

        /// Allocate and copy `src` in — a one-time CPU write into the shared allocation.
        pub fn from_slice(src: &[T]) -> Result<Self, GpuError> {
            let mut buf = Self::new(src.len())?;
            buf.as_mut_slice().copy_from_slice(src);
            Ok(buf)
        }

        pub fn as_slice(&self) -> &[T] {
            // SAFETY: managed memory is host-addressable; ptr/len describe the allocation.
            unsafe { std::slice::from_raw_parts(self.ptr as *const T, self.len) }
        }

        pub fn as_mut_slice(&mut self) -> &mut [T] {
            // SAFETY: `&mut self` gives exclusive access; managed memory is host-addressable.
            unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut T, self.len) }
        }

        /// Device pointer for CUDA kernels — the same bytes the CPU sees (zero-copy).
        pub fn device_ptr(&self) -> u64 {
            self.ptr
        }

        /// Consume the buffer, returning its raw device pointer and element count
        /// **without freeing**. Ownership transfers to the caller — e.g. cudarc's
        /// `upgrade_device_ptr`, whose `CudaSlice` then frees it on drop. This prevents
        /// a double free against this type's `Drop`.
        pub fn into_device_ptr(self) -> (sys::CUdeviceptr, usize) {
            let ptr = self.ptr;
            let len = self.len;
            std::mem::forget(self);
            (ptr, len)
        }
    }

    impl<T: Copy> Drop for ManagedBuffer<T> {
        fn drop(&mut self) {
            // SAFETY: `ptr` came from `malloc_managed` and is freed exactly once here.
            unsafe {
                let _ = result::free_sync(self.ptr);
            }
        }
    }

    // The handle uniquely owns its managed allocation.
    unsafe impl<T: Copy + Send> Send for ManagedBuffer<T> {}
    unsafe impl<T: Copy + Sync> Sync for ManagedBuffer<T> {}
}

#[cfg(feature = "cuda")]
pub use managed::ManagedBuffer;

#[cfg(all(test, feature = "cuda"))]
mod tests {
    use super::*;
    use crate::CudaGpuContext;

    #[test]
    fn managed_roundtrip_or_skip() {
        if !CudaGpuContext::is_available() {
            eprintln!("[um-test] no CUDA device — skipping");
            return;
        }
        // A CUDA context must be current before allocating managed memory.
        let _ctx = CudaGpuContext::try_global().expect("CUDA context should initialize");

        let src: Vec<u32> = (0..4096).collect();
        let mut buf = ManagedBuffer::<u32>::from_slice(&src).expect("managed alloc");

        // The CPU reads the very bytes a kernel would read via device_ptr() — one copy.
        assert_eq!(buf.as_slice(), src.as_slice());
        assert_ne!(buf.device_ptr(), 0, "managed device pointer must be non-null");

        // Mutate in place from the CPU (no host<->device copy) and observe it.
        buf.as_mut_slice()[0] = 42;
        assert_eq!(buf.as_slice()[0], 42);
    }

    #[test]
    fn unified_buffer_falls_back_to_host_without_um() {
        // Without SAMYAMA_GPU_UM the factory returns a host buffer, unconditionally usable.
        std::env::remove_var("SAMYAMA_GPU_UM");
        let b = UnifiedBuffer::from_slice(&[1u32, 2, 3]);
        assert!(!b.is_managed());
        assert_eq!(b.as_slice(), &[1, 2, 3]);
    }
}
