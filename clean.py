from confluent_kafka.schema_registry import SchemaRegistryClient
import config


def delete_subjects():
    print("Cleaning former schemes.")

    # Connect to Registry
    client = SchemaRegistryClient(config.sr_conf)

    subjects_to_delete = ['energy_stream-value', 'weather_stream-value']

    for subject in subjects_to_delete:
        try:
            # First, soft delete - removes versions
            versions = client.delete_subject(subject)
            print(f"'{subject}'s versions are cleaned: {versions}")

            # Second, hard delete
            client.delete_subject(subject, permanent=True)
            print(f"'{subject}' has been removed completely.")

        except Exception as e:
            print(f"'{subject}' couldn't be removed. It may not be there at all: {e}")


if __name__ == "__main__":
    delete_subjects()