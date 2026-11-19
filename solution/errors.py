"""Error codes for the permission revocation coordinator.

Protocol/usage failures raise AppError with one of these codes.  A *business*
outcome such as a stale plan is NOT an error: apply() returns a result dict
with status "stale" and persists a stale receipt, leaving business state
(current permissions) untouched.
"""


class AppError(Exception):
    def __init__(self, code, message, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message}


# --- error codes -------------------------------------------------------------
VALIDATION_ERROR = "VALIDATION_ERROR"          # malformed input parameters
OBJECT_NOT_FOUND = "OBJECT_NOT_FOUND"          # referenced object does not exist
DUPLICATE_OBJECT = "DUPLICATE_OBJECT"          # create_object on existing object
LEASE_NOT_FOUND = "LEASE_NOT_FOUND"            # remove_lease on unknown lease
DUPLICATE_LEASE = "DUPLICATE_LEASE"            # add_lease reusing a lease id
ATTR_REF_NOT_FOUND = "ATTR_REF_NOT_FOUND"      # remove_attr_ref on unknown attribute
DEPENDENCY_NOT_FOUND = "DEPENDENCY_NOT_FOUND"  # remove_dependency on unknown edge
DUPLICATE_DEPENDENCY = "DUPLICATE_DEPENDENCY"  # add_dependency on existing edge
DEPENDENCY_CYCLE = "DEPENDENCY_CYCLE"          # dependency would close a cycle (DAG violated)
PLAN_NOT_FOUND = "PLAN_NOT_FOUND"              # apply on unknown plan id
PLAN_NOT_PENDING = "PLAN_NOT_PENDING"          # plan already applied / marked stale
REQUEST_ID_CONFLICT = "REQUEST_ID_CONFLICT"    # request id reused with different parameters
