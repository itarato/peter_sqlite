use std::collections::{HashMap, VecDeque};

use crate::{
    btree_page_header::BTreePageHeader,
    cell::{TableBTreeInteriorCell, TableBTreeLeafCell},
    common::{BTreePageType, Incrementer},
    database::Database,
    query::{Query, QueryField},
    reader::Reader,
    record::Record,
};

pub(crate) struct QueryExecutor;

impl QueryExecutor {
    pub(crate) fn execute_query(query: &Query, db: &Database, reader: &Reader<'_, u8>) {
        let table = db.tables.get(&query.source).unwrap();
        let mut query_visitor = match &query.fields {
            QueryField::Count => QueryVisitor::Count(0),
            QueryField::List(fields) => QueryVisitor::Fields(
                fields
                    .iter()
                    .map(|name| table.sql_schema.field_index(name))
                    .collect(),
            ),
        };

        let mut incrementer_map = table.sql_schema.make_incrementer_map();
        let mut offset_stack: VecDeque<usize> = VecDeque::new();
        offset_stack.push_back(db.header.page_size * (table.root_page - 1));

        while let Some(offset) = offset_stack.pop_front() {
            let page_header = BTreePageHeader::from(&reader.at(offset));

            match page_header.kind {
                BTreePageType::LeafTable => {
                    for cell_offset in page_header.cell_offsets {
                        let cell = TableBTreeLeafCell::from(&reader.at(offset + cell_offset));
                        let mut row = cell.payload.read_as_table_row(&table.sql_schema);
                        Self::apply_incrementer(&mut row, &mut incrementer_map);

                        let mut is_match = true;
                        for cond in &query.conditions {
                            let field_index = table.sql_schema.field_index(&cond.lhs);
                            if !cond.op.eval(&row[field_index], &cond.rhs) {
                                is_match = false;
                                break;
                            }
                        }

                        if is_match {
                            query_visitor.signal_on_match(&row);
                        }
                    }
                }

                BTreePageType::InteriorTable => {
                    for cell_offset in page_header.cell_offsets {
                        let cell = TableBTreeInteriorCell::from(&reader.at(offset + cell_offset));
                        offset_stack.push_back((cell.left_child_pointer - 1) * db.header.page_size);
                    }

                    offset_stack.push_back(
                        (page_header.rightmost_pointer.unwrap() - 1) * db.header.page_size,
                    );
                }
                other => unimplemented!("Page type {:?} not implemented", other),
            }
        }

        query_visitor.signal_post_query();
    }

    fn apply_incrementer(
        rows: &mut Vec<Record>,
        incrementer_map: &mut HashMap<usize, Incrementer>,
    ) {
        for (i, inc) in incrementer_map.iter_mut() {
            rows[*i] = Record::I64(inc.next_value() as i64);
        }
    }
}

enum QueryVisitor {
    Count(usize),
    Fields(Vec<usize>),
}

impl QueryVisitor {
    fn signal_on_match(&mut self, row: &Vec<Record>) {
        match self {
            Self::Count(n) => *n += 1,
            Self::Fields(field_indices) => println!(
                "{}",
                field_indices
                    .iter()
                    .map(|i| row[*i].to_string())
                    .collect::<Vec<_>>()
                    .join("|")
            ),
        }
    }

    fn signal_post_query(&self) {
        match self {
            Self::Count(n) => println!("{}", n),
            Self::Fields(_) => {}
        }
    }
}
