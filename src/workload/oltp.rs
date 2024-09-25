#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum OltpRequest {
    // "SELECT c FROM sbtest%u WHERE id=?",
    PointSelect(PointSelect),
}

#[derive(Debug, PartialEq)]
pub struct PointSelect {
    pub id: i32,
    pub table: i32,
}