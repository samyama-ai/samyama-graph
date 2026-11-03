//! Loader stubs for CEC2017 / CEC2022 shift+rotation data.
//!
//! The official CEC test packages ship per-function shift vectors and
//! rotation matrices as text files. This module defines the file layout
//! we expect at `data/cec/{cec2017,cec2022}/M_<id>_D<dim>.txt` (rotation)
//! and `data/cec/{cec2017,cec2022}/shift_data_<id>.txt` (shift).
//!
//! Wiring: download once into the repo's `data/cec/`, then
//! `load_shift_and_rotation(suite, fn_id, dim)` returns the pair.
//! Until populated, the shifted/rotated CEC composition functions are
//! not exposed.

use ndarray::{Array1, Array2};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
pub enum CecSuite { Cec2017, Cec2022 }

impl CecSuite {
    fn dir(&self) -> &'static str {
        match self { CecSuite::Cec2017 => "cec2017", CecSuite::Cec2022 => "cec2022" }
    }
}

pub fn data_root() -> PathBuf {
    std::env::var("SAMYAMA_CEC_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data/cec"))
}

pub fn load_shift(suite: CecSuite, fn_id: u32, dim: usize) -> Option<Array1<f64>> {
    let path = data_root().join(suite.dir()).join(format!("shift_data_{}.txt", fn_id));
    let txt = std::fs::read_to_string(&path).ok()?;
    let vals: Vec<f64> = txt.split_whitespace().filter_map(|s| s.parse().ok()).take(dim).collect();
    if vals.len() == dim { Some(Array1::from(vals)) } else { None }
}

pub fn load_rotation(suite: CecSuite, fn_id: u32, dim: usize) -> Option<Array2<f64>> {
    let path = data_root().join(suite.dir()).join(format!("M_{}_D{}.txt", fn_id, dim));
    let txt = std::fs::read_to_string(&path).ok()?;
    let vals: Vec<f64> = txt.split_whitespace().filter_map(|s| s.parse().ok()).collect();
    if vals.len() == dim * dim {
        Some(Array2::from_shape_vec((dim, dim), vals).ok()?)
    } else {
        None
    }
}
