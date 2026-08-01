use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionObservation {
    pub(crate) label: String,
    pub(crate) advanced: bool,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionProgress {
    current: Option<String>,
}

impl SessionProgress {
    pub(crate) fn observe(
        &mut self,
        event_time: &str,
        labels_by_event_time: &BTreeMap<String, String>,
    ) -> Result<SessionObservation, String> {
        let label = resolve_session_label(event_time, labels_by_event_time)?;
        let advanced = match self.current.as_deref() {
            None => false,
            Some(current) if current == label => false,
            Some(current) if current < label => true,
            Some(current) => {
                return Err(format!(
                    "session labels moved backwards from {current} to {label} at {event_time}"
                ))
            }
        };
        self.current = Some(label.to_string());
        Ok(SessionObservation {
            label: label.to_string(),
            advanced,
        })
    }
}

fn resolve_session_label<'a>(
    event_time: &'a str,
    labels_by_event_time: &'a BTreeMap<String, String>,
) -> Result<&'a str, String> {
    if let Some(label) = labels_by_event_time.get(event_time) {
        if is_session_label(label) {
            return Ok(label);
        }
        return Err(format!(
            "invalid session label {label:?} for event time {event_time}"
        ));
    }
    if is_session_label(event_time) {
        return Ok(event_time);
    }
    Err(format!(
        "event timestamp {event_time} requires an explicit session label"
    ))
}

fn is_session_label(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::SessionProgress;
    use std::collections::BTreeMap;

    #[test]
    fn direct_daily_values_are_their_own_session_labels() {
        let mut progress = SessionProgress::default();
        assert!(
            !progress
                .observe("2024-01-02", &BTreeMap::new())
                .unwrap()
                .advanced
        );
        assert!(
            progress
                .observe("2024-01-03", &BTreeMap::new())
                .unwrap()
                .advanced
        );
    }

    #[test]
    fn event_timestamps_require_explicit_labels() {
        let mut progress = SessionProgress::default();
        assert!(progress
            .observe("2024-01-02T14:31:00Z", &BTreeMap::new())
            .is_err());
    }
}
