use crate::{database::Database, query::Query};

pub(crate) struct QueryExecutor;

impl QueryExecutor {
    pub(crate) fn execute_query(query: &Query, db: &Database) {
        let table = db.tables.get(&query.source).unwrap();

        // Continue
    }
}
