use crate::{
    database::Database,
    query::{Query, QueryField},
    record::Record,
};

pub(crate) struct QueryExecutor;

impl QueryExecutor {
    pub(crate) fn execute_query(query: &Query, db: &Database) {
        let table = db.tables.get(&query.source).unwrap();
        let mut query_visitor = match &query.fields {
            QueryField::Count => QueryVisitor::Count(0),
            QueryField::List(fields) => {
                QueryVisitor::Fields(fields.iter().map(|name| table.field_index(name)).collect())
            }
        };

        'outer: for row in &table.rows {
            for cond in &query.conditions {
                let field_index = table.field_index(&cond.lhs);
                if !cond.op.eval(&row[field_index], &cond.rhs) {
                    continue 'outer;
                }
            }

            query_visitor.signal_on_match(row);
        }
        query_visitor.signal_post_query();
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
