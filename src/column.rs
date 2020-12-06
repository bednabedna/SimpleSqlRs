use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::*;

use super::value::*;

pub type ColumnIndex = HashMap<Value, Vec<usize>>;

struct ColumnData {
    cells: Vec<Value>,
    maybe_index: RefCell<Option<ColumnIndex>>,
}

#[derive(Clone)]
pub struct Column {
    data: Rc<ColumnData>,
}

impl Column {
    pub fn new(cells: Vec<Value>) -> Column {
        Column {
            data: Rc::new(ColumnData {
                cells,
                maybe_index: RefCell::new(None),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.data.cells.len()
    }

    pub fn remap(&self, indices: &[usize]) -> Column {
        let cells = &self.data.cells;
        Column::new(indices.iter().map(|&i| cells[i].clone()).collect())
    }

    pub fn get_index(&self) -> Ref<ColumnIndex> {
        {
            let mut maybe_index = self.data.maybe_index.borrow_mut();
            if maybe_index.is_none() {
                let mut index: HashMap<Value, Vec<_>> =
                    HashMap::with_capacity(self.data.cells.len());
                for (i, cell) in self.data.cells.iter().enumerate() {
                    if let Some(indices) = index.get_mut(cell) {
                        indices.push(i);
                    } else {
                        let mut indices_list = Vec::with_capacity(1);
                        indices_list.push(i);
                        index.insert(cell.clone(), indices_list);
                    }
                }
                index.shrink_to_fit();
                *maybe_index = Some(index);
            }
        }
        Ref::map(self.data.maybe_index.borrow(), |opt_some_index| {
            opt_some_index.as_ref().unwrap()
        })
    }
    pub fn has_index(&self) -> bool {
        self.data.maybe_index.borrow().is_some()
    }

    pub fn cells(&self) -> &[Value] {
        self.data.cells.as_ref()
    }
}
