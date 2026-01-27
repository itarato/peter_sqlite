use log::debug;
use regex::Regex;

use crate::record::Record;

#[derive(Debug)]
pub(crate) enum QueryField {
    Count,
    List(Vec<String>),
}

#[derive(Debug)]
pub(crate) enum QueryConditionOp {
    Eq,
}

impl QueryConditionOp {
    pub(crate) fn eval(&self, lhs: &Record, rhs: &Record) -> bool {
        // debug!("LHS={:?} RHS={:?}", &lhs, &rhs);
        match self {
            Self::Eq => {
                match (lhs.as_int(), rhs.as_int()) {
                    (Some(a), Some(b)) => return a == b,
                    _ => {}
                }
                match (lhs.as_str(), rhs.as_str()) {
                    (Some(a), Some(b)) => return a == b,
                    _ => {}
                }
                false
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct QueryCondition {
    pub(crate) lhs: String,
    pub(crate) op: QueryConditionOp,
    pub(crate) rhs: Record,
}

#[derive(Debug)]
pub(crate) struct Query {
    pub(crate) fields: QueryField,
    pub(crate) source: String,
    pub(crate) conditions: Vec<QueryCondition>,
}

impl Query {
    pub(crate) fn parse(raw: &str) -> Self {
        let query_re =
            Regex::new(r#"(?i)SELECT\s+(.*)\s+FROM\s+(\w+)\s*(\s+WHERE\s+(.*))?$"#).unwrap();
        let caps = query_re.captures(raw).unwrap().iter().collect::<Vec<_>>();

        let fields_raw = caps[1].unwrap().as_str();
        let fields = if fields_raw.to_lowercase().starts_with("count(") {
            QueryField::Count
        } else {
            let field_parts = fields_raw
                .split(',')
                .map(|elem| elem.trim().to_string())
                .collect::<Vec<_>>();
            QueryField::List(field_parts)
        };

        let source = caps[2].unwrap().as_str().trim().to_string();

        let conditions = match caps[4] {
            Some(m) => m
                .as_str()
                .split("AND")
                .map(|elem| Self::parse_condition(elem))
                .collect::<Vec<_>>(),
            None => vec![],
        };

        Self {
            fields,
            source,
            conditions,
        }
    }

    fn parse_condition(raw: &str) -> QueryCondition {
        let parts = raw.split('=').collect::<Vec<_>>();
        assert_eq!(2, parts.len());
        let lhs = parts[0].trim().to_string();
        let op = QueryConditionOp::Eq;
        let rhs = Record::parse(parts[1].trim());

        QueryCondition { lhs, op, rhs }
    }
}

#[cfg(test)]
mod test {
    use crate::query::Query;

    #[test]
    fn test_query_parse() {
        dbg!(Query::parse("SELECT COUNT(*) FROM apples"));
        dbg!(Query::parse(
            "SELECT COUNT(*) FROM apples WHERE name = 'mariogold'"
        ));
        dbg!(Query::parse(
            "SELECT name, date FROM apples WHERE name = 'mariogold' AND age = 123"
        ));
    }
}
