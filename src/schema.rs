use crate::common::Error;
use regex::Regex;

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
}

#[derive(Debug)]
pub(crate) struct TableSchema {
    pub(crate) name: String,
    pub(crate) fields: Vec<TableField>,
}

impl TableSchema {
    pub(crate) fn from(raw: &str) -> Result<Self, Error> {
        let table_regex = Regex::new(r"(?s)CREATE\s+TABLE\s+(\w+)\s*\((.*)\)")?;

        let caps = table_regex
            .captures(raw)
            .expect("Failed capturing")
            .iter()
            .map(|elem| elem.unwrap())
            .collect::<Vec<_>>();
        if caps.len() != 3 {
            return Err("Cannot find name and fields".into());
        }

        let name = caps[1].as_str();

        if name == "sqlite_sequence" {
            return Ok(TableSchema {
                name: name.to_string(),
                fields: vec![],
            });
        }

        let raw_fields_str = caps[2].as_str();
        let raw_field_list = raw_fields_str
            .split(',')
            .map(|s| s.trim())
            .collect::<Vec<_>>();
        let mut fields = vec![];

        for raw_field in raw_field_list {
            let parts = raw_field.split(char::is_whitespace).collect::<Vec<_>>();
            if parts.len() < 2 {
                return Err("Field def not recognized".into());
            }

            let name = parts[0].to_string();
            let mut kind = TableFieldKind::from(parts[1]);

            let suffix = parts[2..].join(" ");
            let primary_key = suffix.contains("primary key");
            if suffix.contains("autoincrement") {
                kind.to_auto_increment();
            }

            fields.push(TableField {
                name,
                kind,
                primary_key,
            });
        }

        Ok(TableSchema {
            name: name.to_string(),
            fields,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::schema::TableSchema;

    #[test]
    fn test_from() {
        dbg!(TableSchema::from(
            "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)"
        ));

        dbg!(TableSchema::from("CREATE TABLE sqlite_sequence(name,seq)"));

        dbg!(TableSchema::from(
            "CREATE TABLE oranges\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tdescription text\n)"
        ));
    }
}
