//! `Value` is the executor's universal cell, so its width is paid everywhere
//! (#570).
//!
//! Every binding in every record is one. Every hash table that groups or joins
//! on one is at least that wide per entry. `Value` was **144 bytes**, because
//! `Value::Node` carried a whole `Node` inline — and `Node` is 128 bytes, a
//! `HashSet<Label>` and a `PropertyMap` held inline plus id, version and two
//! timestamps. Late materialization (ADR-012) exists so that variant stays
//! rare; it was still setting the size of the type for everything else.
//!
//! These are guards, not designs. A `Box` removed from `Value::Node`, or a
//! large field added to `PropertyValue`, would be invisible in every other test
//! and would quietly make every record wider again.

use samyama::graph::{Node, PropertyValue};
use samyama::query::executor::{Record, Value};

/// Set by `Property(PropertyValue)`, which is as small as `Value` can be
/// without shrinking `PropertyValue` itself.
#[test]
fn a_value_is_no_wider_than_a_property_value() {
    let value = std::mem::size_of::<Value>();
    let property = std::mem::size_of::<PropertyValue>();
    assert_eq!(
        value, property,
        "Value is {value} bytes against PropertyValue's {property} — some other \
         variant has grown past it. `Value::Node` and `Value::Edge` are boxed \
         deliberately (#570); check nothing has unboxed them."
    );
}

#[test]
fn a_value_stays_under_the_ceiling_it_was_brought_to() {
    // 144 before #570. The exact figure matters less than that it does not
    // drift back, so this is a ceiling rather than an equality.
    let value = std::mem::size_of::<Value>();
    assert!(
        value <= 64,
        "Value is {value} bytes; it was brought down to 56 from 144 and every \
         record binding pays it"
    );
}

#[test]
fn boxing_did_not_change_what_a_node_value_holds() {
    // The representation changed; the behaviour must not. A materialised node
    // still answers for its id, its labels and its properties.
    let mut node = Node::new(samyama::graph::NodeId::new(7), samyama::graph::Label::new("Person"));
    node.set_property("name", PropertyValue::String("Ada".into()));

    let value = Value::Node(samyama::graph::NodeId::new(7), Box::new(node));
    assert!(value.is_node());
    assert_eq!(value.node_id(), Some(samyama::graph::NodeId::new(7)));

    // And it compares and hashes by id, as it did before (`NodeRef` equals
    // `Node` with the same id).
    assert_eq!(value, Value::NodeRef(samyama::graph::NodeId::new(7)));
    assert_ne!(value, Value::NodeRef(samyama::graph::NodeId::new(8)));
}

#[test]
fn a_record_binding_costs_a_value_plus_its_name() {
    // What the width actually buys, stated so the number has a meaning:
    // a three-binding row is this much memory to clone per derived row (#562).
    let per_binding = std::mem::size_of::<(std::sync::Arc<str>, Value)>();
    assert!(
        per_binding <= 80,
        "a binding is {per_binding} bytes; at 144-byte Values it was 160"
    );

    let mut record = Record::new();
    record.bind("a", Value::NodeRef(samyama::graph::NodeId::new(1)));
    record.bind("b", Value::Property(PropertyValue::Integer(2)));
    assert_eq!(record.bindings().len(), 2);
}
