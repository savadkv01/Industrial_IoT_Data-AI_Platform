from feast import Entity
from feast.value_type import ValueType

machine = Entity(
    name="machine",
    join_keys=["machine_id"],
    value_type=ValueType.STRING,
    description="Industrial machine identifier",
)