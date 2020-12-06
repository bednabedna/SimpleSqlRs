use super::column::Column;
use super::value::Value;
use std::collections::HashMap;
use std::collections::HashSet;

pub struct Op {
    column_name: String,
    operation: Function,
}

type Function = Box<dyn Fn(&[&str]) -> String>;

impl Op {
    pub fn new(column_name: &str, operation: Function) -> Op {
        Op {
            column_name: column_name.into(),
            operation,
        }
    }
}

pub struct MiOp {
    out_column: String,
    in_columns: Vec<String>,
    operation: Function,
}

impl MiOp {
    pub fn new(input: &[&str], output: &str, operation: Function) -> MiOp {
        MiOp {
            in_columns: input.iter().map(|v| String::from(*v)).collect(),
            out_column: String::from(output),
            operation,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    columns: HashMap<Value, Column>,
}

pub type Res<T> = Result<T, String>;

impl Table {
    pub fn load_tsv(path: &str, skip_lines: usize) -> Res<Table> {
        let input = std::fs::read_to_string(path)
            .map_err(|err| format!("Errore leggendo tsv file {}: {}", path, err))?;
        Table::parse_tsv(input.as_ref(), skip_lines)
            .map_err(|err| format!("Errore creando la table dal tsv file {}: {}", path, err))
    }

    pub fn parse_tsv(input: &str, skip_lines: usize) -> Res<Table> {
        let mut lines = input.lines().skip(skip_lines).skip_while(|&l| l.is_empty());
        if let Some(header) = lines.next() {
            let mut builder = TableBuilder::new(
                header
                    .split('\t')
                    .map(|col_name| col_name.trim().into())
                    .collect(),
            );

            for line in lines {
                if !line.is_empty() {
                    builder =
                        builder.add_row(line.split('\t').map(|s| s.trim().into()).collect())?;
                }
            }

            Ok(builder.build())
        } else {
            Err(String::from("mancano i nomi di colonna"))
        }
    }

    pub fn columns_count(&self) -> usize {
        self.columns.len()
    }

    pub fn rows_count(&self) -> usize {
        self.columns.values().next().map(|c| c.len()).unwrap_or(0)
    }

    pub fn column(&self, col_name: &str) -> Res<Column> {
        if let Some(column) = self.columns.get(col_name) {
            Ok(column.clone())
        } else {
            Err(format!("colonna '{}' non esiste", col_name))
        }
    }

    pub fn select_columns(&self, col_names: &[&str]) -> Res<Table> {
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(col_names.len());
        for &col_name in col_names {
            columns.insert(Value::from(col_name), self.column(col_name)?);
        }
        Ok(Table { columns })
    }

    pub fn deselect_column(&self, col_name: &str) -> Res<Table> {
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(self.columns.len());
        for (colmun_name, column) in self.columns.iter() {
            if col_name != colmun_name.as_str() {
                columns.insert(colmun_name.clone(), column.clone());
            }
        }
        if columns.len() != self.columns.len() - 1 {
            Err(format!("colonna '{}' non esiste", col_name))
        } else {
            Ok(Table { columns })
        }
    }

    pub fn rename_column(&self, old_col_name: &str, new_col_name: &str) -> Res<Table> {
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(self.columns.len());
        let mut not_found = true;
        for (colmun_name, column) in self.columns.iter() {
            let name = if old_col_name == colmun_name.as_str() {
                not_found = false;
                Value::from(new_col_name)
            } else {
                colmun_name.clone()
            };
            columns.insert(name, column.clone());
        }
        if not_found {
            Err(format!("colonna '{}' non esiste", old_col_name))
        } else {
            Ok(Table { columns })
        }
    }

    fn remap(&self, positions: &[usize]) -> Table {
        let mut columns = HashMap::with_capacity(self.columns.len());
        for (col_name, col) in self.columns.iter() {
            columns.insert(col_name.clone(), col.remap(positions));
        }
        Table { columns }
    }

    pub fn filter_column(&self, col_name: &str, filter: impl Fn(&str) -> bool) -> Res<Table> {
        let column = self.column(col_name)?;
        let retained_positions: Vec<usize> = column
            .cells()
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if filter(v) { Some(i) } else { None })
            .collect();

        Ok(if retained_positions.len() == self.rows_count() {
            self.clone()
        } else {
            self.remap(&retained_positions)
        })
    }

    pub fn diff_on_columns(
        &self,
        col_name_self: &str,
        other: &Table,
        col_name_other: &str,
    ) -> Res<Table> {
        let column_self = self.column(col_name_self)?;
        let column_other = other.column(col_name_other)?;
        let other_index = column_other.get_index();
        let retained_positions: Vec<usize> = column_self
            .cells()
            .iter()
            .enumerate()
            .filter_map(|(position, value)| {
                if !other_index.contains_key(value) {
                    Some(position)
                } else {
                    None
                }
            })
            .collect();
        Ok(self.remap(&retained_positions))
    }

    pub fn map_column(&self, col_name: &str, map: impl Fn(&str) -> String) -> Res<Table> {
        let col = self.column(col_name)?;
        let column_cells = col.cells();
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(self.columns_count());
        for (cn, column) in self.columns.iter() {
            let new_column = if col_name == cn.as_str() {
                let mapped_cells = column_cells.iter().map(|v| Value::new(map(v))).collect();
                Column::new(mapped_cells)
            } else {
                column.clone()
            };
            columns.insert(cn.clone(), new_column);
        }
        Ok(Table { columns })
    }

    pub fn dinstinct_column(&self, col_name: &str) -> Res<Table> {
        let col = self.column(col_name)?;
        let mut found: HashSet<&str> = HashSet::with_capacity(self.rows_count());
        let positions: Vec<usize> = col
            .cells()
            .iter()
            .enumerate()
            .filter_map(|(position, value)| {
                if found.contains(value.as_str()) {
                    None
                } else {
                    found.insert(value.as_str());
                    Some(position)
                }
            })
            .collect();
        Ok(self.remap(&positions))
    }

    pub fn sort_column(&self, col_name: &str) -> Res<Table> {
        let col = self.column(col_name)?;
        let mut values_with_pos = col.cells().iter().enumerate().collect::<Vec<_>>();
        values_with_pos.sort_by_key(|(_, value)| *value);
        let new_order: Vec<usize> = values_with_pos.into_iter().map(|(pos, _)| pos).collect();
        Ok(self.remap(&new_order))
    }

    pub fn sort_column_by(
        &self,
        col_name: &str,
        order: impl Fn(&str, &str) -> std::cmp::Ordering,
    ) -> Res<Table> {
        let col = self.column(col_name)?;
        let mut values_with_pos = col.cells().iter().enumerate().collect::<Vec<_>>();
        values_with_pos.sort_by(|(_, v1), (_, v2)| order(*v1, *v2));
        let new_order: Vec<usize> = values_with_pos.into_iter().map(|(pos, _)| pos).collect();
        Ok(self.remap(&new_order))
    }

    pub fn concatenate(&self, other: &Table) -> Res<Table> {
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(self.columns_count());
        let new_rows_count = self.rows_count() + other.rows_count();
        for (col_name, col) in self.columns.iter() {
            let mut cells: Vec<Value> = Vec::with_capacity(new_rows_count);
            for cell in col.cells() {
                cells.push(cell.clone());
            }
            let other_col = other.column(col_name).map_err(|_| {
                format!(
                    "la seconda table in concatenazione non ha la colonna '{}'",
                    col_name.as_str()
                )
            })?;
            for cell in other_col.cells() {
                cells.push(cell.clone());
            }
            columns.insert(col_name.clone(), Column::new(cells));
        }
        Ok(Table { columns })
    }

    pub fn create_fixed_column(&self, col_name: &str, fixed_value: &str) -> Table {
        let value = Value::new(fixed_value.to_string());
        let cells: Vec<Value> = (0..self.rows_count()).map(|_| value.clone()).collect();

        let mut clone = self.clone();
        clone
            .columns
            .insert(Value::new(col_name.to_string()), Column::new(cells));
        clone
    }

    pub fn create_column(&self, expr: MiOp) -> Res<Table> {
        let mut inputs_cols = Vec::with_capacity(expr.out_column.len());
        for cname in expr.in_columns.into_iter() {
            let col = self.column(cname.as_ref())?;
            inputs_cols.push(col);
        }
        let function = expr.operation;
        let col_rows = (0..self.rows_count())
            .map(|position| {
                let args: Vec<&str> = inputs_cols
                    .iter()
                    .map(|col| col.cells()[position].as_str())
                    .collect();
                let value = (function)(args.as_slice());
                Value::new(value)
            })
            .collect();
        let mut clone = self.clone();
        let out_col_name = expr.out_column;
        clone
            .columns
            .insert(Value::new(out_col_name), Column::new(col_rows));
        Ok(clone)
    }

    pub fn concatenate_columns(
        &self,
        col_1: &str,
        separator: char,
        col_2: &str,
        new_col: &str,
    ) -> Res<Table> {
        let col_1 = self.column(col_1)?;
        let col_2 = self.column(col_2)?;
        let cells: Vec<Value> = col_1
            .cells()
            .iter()
            .zip(col_2.cells().iter())
            .map(|(a, b)| {
                let mut result = String::with_capacity(a.len() + 1 + b.len());
                result += a;
                result.push(separator);
                result += b;
                Value::new(result)
            })
            .collect();

        let mut clone = self.clone();
        clone
            .columns
            .insert(Value::new(new_col.to_string()), Column::new(cells));
        Ok(clone)
    }

    pub fn join_on_columns(
        &self,
        col_name_self: &str,
        other: &Table,
        col_name_other: &str,
    ) -> Res<Table> {
        let column_self = self.column(col_name_self)?;
        let column_other = other.column(col_name_other)?;

        if column_self.has_index()
            || (!column_other.has_index() && column_self.len() <= column_other.len())
        {
            // join using/building index on self
            let mut remapped_positions_self: Vec<usize> = Vec::with_capacity(column_self.len());
            let mut remapped_positions_other: Vec<usize> = Vec::with_capacity(column_self.len());
            let self_index = column_self.get_index();
            for (position, other_value) in column_other.cells().iter().enumerate() {
                if let Some(self_positions_with_other_value) = self_index.get(other_value) {
                    remapped_positions_self.extend(self_positions_with_other_value);
                    let additions = self_positions_with_other_value.len();
                    remapped_positions_other.reserve(additions);
                    for _ in 0..additions {
                        remapped_positions_other.push(position);
                    }
                }
            }
            let mut table1 = self.remap(&remapped_positions_self);
            let table2 = other.remap(&remapped_positions_other);
            table1.columns.extend(table2.columns);
            Ok(table1)
        } else {
            // join building index on other
            other.join_on_columns(col_name_other, self, col_name_self)
        }
    }

    pub fn group_by_column(&self, col_name: &str, column_operations: &[Op]) -> Res<Table> {
        let group_column = self.column(col_name)?;
        let mut columns: HashMap<Value, _> = HashMap::with_capacity(self.columns.len());
        let groups_index = group_column.get_index();
        for op in column_operations {
            let column_operation: &str = op.column_name.as_ref();
            let col = self.column(column_operation)?;
            let column_cells = col.cells();
            let new_column_cells = groups_index
                .values()
                .map(|positions| {
                    let items: Vec<&str> = positions
                        .iter()
                        .map(|&p| column_cells[p].as_str())
                        .collect();
                    Value::new((op.operation)(items.as_slice()))
                })
                .collect();
            columns.insert(column_operation.into(), Column::new(new_column_cells));
        }
        for col_name in self.columns.keys() {
            if !columns.contains_key(col_name) {
                let col = self.column(col_name.as_ref())?;
                let column_cells = col.cells();
                let new_column_cells: Vec<Value> = groups_index
                    .values()
                    .map(|positions| column_cells[positions[0]].clone())
                    .collect();
                columns.insert(col_name.clone(), Column::new(new_column_cells));
            }
        }
        Ok(Table { columns })
    }

    pub fn to_tsv(&self, header: Vec<String>) -> Res<String> {
        let mut cols = Vec::with_capacity(self.columns_count());
        for col_name in header.iter() {
            cols.push(self.column(col_name)?);
        }

        let tsv = std::iter::once(header.join("\t"))
            .chain((0..self.rows_count()).map(|row| {
                cols.iter()
                    .map(|col| col.cells()[row].as_str())
                    .collect::<Vec<&str>>()
                    .join("\t")
            }))
            .collect::<Vec<String>>()
            .join("\n");

        Ok(tsv)
    }

    pub fn write_tsv_file(&self, path: &str, header: Vec<String>) -> Res<()> {
        std::fs::write(path, self.to_tsv(header)?).map_err(|e| {
            format!(
                "Impossibile scrivere su file tsv '{}' la table: {}",
                path,
                e.to_string()
            )
        })?;
        Ok(())
    }

    pub fn to_repr(&self) -> String {
        let mut witdh_sums = 0;
        let mut col_widths: Vec<(&str, isize)> = self
            .columns
            .iter()
            .map(|(col_name, col)| {
                let max_item_len = col
                    .cells()
                    .iter()
                    .map(|cell| cell.chars().count())
                    .max()
                    .unwrap_or(0);
                let col_width = col_name.chars().count().max(max_item_len) as isize;
                witdh_sums += col_width;
                (col_name.as_ref(), col_width)
            })
            .collect();
        col_widths.sort_unstable();
        let rows = self.columns.iter().next().map_or(0, |v| v.1.len());
        let width = witdh_sums + 3 * (self.columns.len() as isize) + 1;
        let buffer_size = (rows + 3) * (width + (self.columns.len() * 2) as isize) as usize;

        let mut result = String::with_capacity(buffer_size);
        for _ in 0..width {
            result.push('_');
        }
        result += "\n| ";
        for (value, w) in col_widths.iter() {
            result += value;
            let padding = w - (value.chars().count() as isize);
            for _ in 0..padding {
                result.push(' ');
            }
            result += " | ";
        }
        result += "\n|";
        for (i, (_, w)) in col_widths.iter().enumerate() {
            for _ in 0..(*w + 2) {
                result.push('-');
            }
            if i < self.columns_count() - 1 {
                result.push('+');
            }
        }
        result.push('|');
        if rows > 0 {
            result += "\n| ";
        }
        let col_widths: Vec<(&[Value], isize)> = col_widths
            .into_iter()
            .map(|(c, w)| (self.columns.get(c).unwrap().cells(), w))
            .collect();
        for row in 0..rows {
            for (values, w) in col_widths.iter() {
                let value = values[row].as_ref();
                result += value;
                let padding = w - (value.chars().count() as isize);
                for _ in 0..padding {
                    result.push(' ');
                }
                result += " | ";
            }
            if row < rows - 1 {
                result += "\n| ";
            }
        }
        result += "\n|";
        for _ in 0..width - 2 {
            result.push('_');
        }
        result += "|\n";
        result
    }
}

pub struct TableBuilder {
    columns: Vec<(Value, Vec<Value>)>,
}
impl TableBuilder {
    pub fn new(column_names: Vec<String>) -> TableBuilder {
        TableBuilder {
            columns: column_names
                .into_iter()
                .map(|n| (Value::new(n), Vec::new()))
                .collect(),
        }
    }

    pub fn add_row(mut self, cells: Vec<String>) -> Result<TableBuilder, String> {
        if cells.len() != self.columns.len() {
            Err(format!(
                "fornita riga di lunghezza {} ma dovrebbe essere {}",
                cells.len(),
                self.columns.len()
            ))
        } else {
            for (col, cell) in self.columns.iter_mut().zip(cells) {
                col.1.push(Value::new(cell))
            }
            Ok(self)
        }
    }

    pub fn build(self) -> Table {
        let mut new_columns = HashMap::with_capacity(self.columns.len());
        for (col_name, cells) in self.columns {
            new_columns.insert(col_name, Column::new(cells));
        }
        Table {
            columns: new_columns,
        }
    }
}
