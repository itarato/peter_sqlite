use std::{collections::HashMap, panic};

use log::debug;
use regex::Regex;

use crate::common::Incrementer;

#[derive(Debug)]
pub(crate) enum TableFieldKind {
    Int { auto_increment: bool },
    Text,
}

impl TableFieldKind {
    fn from(raw: &str) -> Self {
        match raw.to_lowercase().as_str() {
            "integer" => Self::Int {
                auto_increment: false,
            },
            "text" | "varchar" => Self::Text,
            other => unimplemented!("Field type {} not recognized", other),
        }
    }

    fn to_auto_increment(&mut self) {
        match self {
            Self::Int { auto_increment } => *auto_increment = true,
            _ => panic!("Auto incement can only be set on integer field"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TableField {
    pub(crate) name: String,
    pub(crate) kind: TableFieldKind,
    pub(crate) primary_key: bool,
    pub(crate) allow_null: bool,
}

impl TableField {
    pub(crate) fn is_autoincrement(&self) -> bool {
        match self.kind {
            TableFieldKind::Int { auto_increment } => auto_increment,
            _ => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TableSchema {
    pub(crate) name: String,
    pub(crate) fields: Vec<TableField>,
    field_index_cache: HashMap<String, usize>,
}

impl TableSchema {
    fn new(name: String, fields: Vec<TableField>) -> Self {
        let mut field_index_cache = HashMap::new();
        for (i, field) in fields.iter().enumerate() {
            field_index_cache.insert(field.name.clone(), i);
        }

        Self {
            name,
            fields,
            field_index_cache,
        }
    }

    pub(crate) fn from(raw: &str) -> Self {
        let table_regex = Regex::new(r#"(?s)CREATE\s+TABLE\s+"?(\w+)"?\s*\((.*)\)"#).unwrap();

        // debug!("Parsing table schema: {}", raw);

        let caps = table_regex
            .captures(raw)
            .expect(format!("Failed capturing schema def: {}", raw).as_str())
            .iter()
            .map(|elem| elem.unwrap())
            .collect::<Vec<_>>();
        if caps.len() != 3 {
            panic!("Cannot find name and fields");
        }

        let name = caps[1].as_str();

        if name == "sqlite_sequence" {
            return TableSchema::new(name.to_string(), vec![]);
        }

        let raw_fields_str = caps[2].as_str();
        let raw_field_list = raw_fields_str
            .split(',')
            .map(|s| s.trim())
            .collect::<Vec<_>>();
        let mut fields = vec![];

        for raw_field in raw_field_list {
            let field_re =
                Regex::new(r#"^\s*((?:\")[^"]+(?:\")|[^ ]+)\s+([^ ]+)($|\s+.*)"#).unwrap();
            let caps = field_re
                .captures(raw_field)
                .expect("Failed capturing")
                .iter()
                .map(|elem| elem.unwrap())
                .collect::<Vec<_>>();

            let name = caps[1].as_str();
            let name = if name.starts_with('"') {
                name[1..name.len() - 1].to_string()
            } else {
                name.to_string()
            };
            let mut kind = TableFieldKind::from(caps[2].as_str());

            let suffix = caps[3].as_str();
            let primary_key = suffix.contains("primary key");
            if suffix.contains("autoincrement") {
                kind.to_auto_increment();
            }
            let allow_null = !suffix.contains("not null");

            fields.push(TableField {
                name,
                kind,
                primary_key,
                allow_null,
            });
        }

        TableSchema::new(name.to_string(), fields)
    }

    pub(crate) fn field_index(&self, name: &str) -> usize {
        self.field_index_cache[name]
    }

    pub(crate) fn make_incrementer_map(&self) -> HashMap<usize, Incrementer> {
        let mut map = HashMap::new();
        for (i, field) in self.fields.iter().enumerate() {
            if field.is_autoincrement() {
                map.insert(i, Incrementer::new(1));
            }
        }
        map
    }
}

#[derive(Debug)]
pub(crate) struct IndexField {
    pub(crate) field: String,
    pub(super) ascending: bool,
}

#[derive(Debug)]
pub(crate) struct IndexSchema {
    pub(crate) table: String,
    pub(crate) fields: Vec<IndexField>,
}

impl IndexSchema {
    fn from(raw: &str) -> Self {
        unimplemented!()
    }
}

pub(crate) enum Schema {
    Index(IndexSchema),
    Table(TableSchema),
}

impl Schema {
    pub(crate) fn from(raw: &str) -> Self {
        debug!("Schema is parsed: {}", raw);

        if raw.to_lowercase().contains("create index") {
            Self::Index(IndexSchema::from(raw))
        } else if raw.to_lowercase().contains("create table") {
            Self::Table(TableSchema::from(raw))
        } else {
            panic!("Schema not recognized")
        }
    }
}

#[cfg(test)]
mod test {
    use crate::schema::TableSchema;

    #[test]
    fn test_schema_from() {
        dbg!(TableSchema::from(
            "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)"
        ));

        dbg!(TableSchema::from("CREATE TABLE sqlite_sequence(name,seq)"));

        dbg!(TableSchema::from(
            "CREATE TABLE oranges\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tdescription text\n)"
        ));

        dbg!(TableSchema::from(
            "CREATE TABLE oranges\n(\n\t\"id multiple words\" integer primary key autoincrement,\n\tname text,\n\tdescription text\n)"
        ));
    }
}
