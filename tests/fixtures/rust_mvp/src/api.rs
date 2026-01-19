mod inner;

pub struct Api {
    value: i32,
}

impl Api {
    pub fn new(value: i32) -> Self {
        Self { value }
    }
}

pub fn api_fn() {}
