use super::*;

impl From<&workload::client::Replace> for RequestWithValidator {
    fn from(other: &workload::client::Replace) -> Self {
        REPLACE.increment();
        RequestWithValidator {
            request: Request::replace(
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
            REPLACE_STORED.increment();
            Ok(())
        }
        Response::NotStored(_) => {
            REPLACE_NOT_STORED.increment();
            Ok(())
        }
        _ => {
            REPLACE_EX.increment();
            Err(())
        }
    }
}
