use super::*;

impl From<&workload::client::Delete> for RequestWithValidator {
    fn from(other: &workload::client::Delete) -> Self {
        DELETE.increment();
        RequestWithValidator {
            request: Request::delete((*other.key).to_owned().into_boxed_slice(), false),
            validator: Box::new(validate_response),
        }
    }
}

pub fn validate_response(response: Response) -> std::result::Result<(), ()> {
    match response {
        Response::Deleted(_) => {
            DELETE_DELETED.increment();
            Ok(())
        }
        Response::NotFound(_) => {
            DELETE_NOT_FOUND.increment();
            Ok(())
        }
        _ => {
            DELETE_EX.increment();
            Err(())
        }
    }
}
