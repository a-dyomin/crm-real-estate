from enum import Enum


class UserRole(str, Enum):
    admin = "admin"
    call_center = "call_center"
    sales = "sales"
    agent = "agent"
    manager = "manager"


class PropertyDealType(str, Enum):
    sale = "sale"
    rent = "rent"


class PropertyType(str, Enum):
    office = "office"
    retail = "retail"
    warehouse = "warehouse"
    industrial = "industrial"
    land = "land"
    mixed_use = "mixed_use"
    other = "other"


class SourceChannel(str, Enum):
    avito = "avito"
    cian = "cian"
    domclick = "domclick"
    telegram = "telegram"
    manual = "manual"


class ContactIntent(str, Enum):
    seller = "seller"
    tenant = "tenant"
    investor = "investor"
    unknown = "unknown"


class LeadStatus(str, Enum):
    new = "new"
    qualified = "qualified"
    disqualified = "disqualified"
    appointment_set = "appointment_set"
    converted = "converted"


class DealStatus(str, Enum):
    new = "new"
    negotiation = "negotiation"
    due_diligence = "due_diligence"
    closed_won = "closed_won"
    closed_lost = "closed_lost"


class ParserResultStatus(str, Enum):
    new = "new"
    possible_duplicate = "possible_duplicate"
    duplicate = "duplicate"
    converted_to_lead = "converted_to_lead"
    converted_to_deal = "converted_to_deal"
    rejected = "rejected"


class ParserRunStatus(str, Enum):
    running = "running"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"


class CallDirection(str, Enum):
    inbound = "inbound"
    outbound = "outbound"


class CallStatus(str, Enum):
    ringing = "ringing"
    in_progress = "in_progress"
    completed = "completed"
    missed = "missed"
    failed = "failed"


class TranscriptStatus(str, Enum):
    none = "none"
    processing = "processing"
    completed = "completed"
    failed = "failed"
