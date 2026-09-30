use super::find_best_worst;
use crate::common::Individual;
use ndarray::array;

fn ind(f: f64) -> Individual {
    Individual::new(array![f], f)
}

#[test]
fn find_best_worst_locates_minimum_and_maximum_anywhere_in_the_slice() {
    let pop = vec![ind(3.0), ind(-1.0), ind(7.0), ind(0.5)];
    assert_eq!(find_best_worst(&pop), (1, 2));
}

#[test]
fn find_best_worst_keeps_the_first_index_on_ties() {
    let pop = vec![ind(2.0), ind(2.0), ind(2.0)];
    assert_eq!(find_best_worst(&pop), (0, 0));
}
