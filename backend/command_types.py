"""Command queue vocabulary shared by the VPS API and the trusted worker."""

CREATE_SECTION = "create_section"
PROCESS_PRICE = "process_price"
DELETE_SECTION = "delete_section"
DELETE_TASK = "delete_task"
DELETE_ALL_BUY = "delete_all_buy"
DELETE_ENGINE = "delete_engine"
INIT_ENGINE = "init_engine"
ORDERS_SYNC = "orders_sync"
ORDERS_CANCEL_ALL = "orders_cancel_all"

ALLOWED_COMMANDS = {
    CREATE_SECTION,
    PROCESS_PRICE,
    DELETE_SECTION,
    DELETE_TASK,
    DELETE_ALL_BUY,
    DELETE_ENGINE,
    INIT_ENGINE,
    ORDERS_SYNC,
    ORDERS_CANCEL_ALL,
}
