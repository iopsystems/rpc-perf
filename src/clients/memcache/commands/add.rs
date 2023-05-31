use super::*;

impl From<&workload::client::Add> for RequestWithValidator {
    fn from(other: &workload::client::Add) -> Self {
        ADD.increment();
        RequestWithValidator {
            request: Request::add(
                (*other.key).to_owned().into_boxed_slice(),
                (*other.value).to_owned().into_boxed_slice(),
                0,
                Ttl::none(),
                false,
            ),
            validator: Box::new(validate_response),
        }
    }
}

pub fn validate_response(response: Response) -> std::result::Result<(), ()> {
    match response {
        Response::Stored(_) => {
            ADD_STORED.increment();
            Ok(())
        }
        Response::NotStored(_) => {
            ADD_NOT_STORED.increment();
            Ok(())
        }
        _ => {
            ADD_EX.increment();
            Err(())
        }
    }
}
