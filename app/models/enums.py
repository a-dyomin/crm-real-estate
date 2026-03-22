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
    yandex = "yandex"
    bankrupt = "bankrupt"
    web = "web"
    manual = "manual"


class ContactIntent(str, Enum):
    seller = "seller"
    tenant = "tenant"
    investor = "investor"
    unknown = "unknown"


class LeadStatus(str, Enum):
    new_lead = "new_lead"
    qualification = "qualification"
    no_answer = "no_answer"
    call_center_tasks = "call_center_tasks"
    sent_to_commission = "sent_to_commission"
    final_no_answer = "final_no_answer"
    deferred_demand = "deferred_demand"
    poor_quality_lead = "poor_quality_lead"
    high_quality_lead = "high_quality_lead"
    # Legacy statuses kept for backward compatibility with historical data.
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


class DiscoverySeedType(str, Enum):
    domain = "domain"
    url = "url"
    telegram_channel = "telegram_channel"
    keyword = "keyword"


class DiscoveredSourceType(str, Enum):
    classifieds = "classifieds"
    agency = "agency"
    developer = "developer"
    business_center = "business_center"
    mall = "mall"
    auction = "auction"
    government = "government"
    telegram = "telegram"
    directory = "directory"
    aggregator = "aggregator"
    unknown = "unknown"


class DiscoveryStatus(str, Enum):
    new = "new"
    classified = "classified"
    matched = "matched"
    ready_for_activation = "ready_for_activation"
    active = "active"
    paused = "paused"
    rejected = "rejected"
    error = "error"


class OnboardingPriority(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    urgent = "urgent"


class DiscoveryRunStatus(str, Enum):
    running = "running"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"


class SourceLinkType(str, Enum):
    outbound = "outbound"
    listing = "listing"
    sitemap = "sitemap"
    social = "social"
    unknown = "unknown"


class SourceHealthStatus(str, Enum):
    new = "new"
    healthy = "healthy"
    degraded = "degraded"
    blocked = "blocked"
    failed = "failed"


class SourceState(str, Enum):
    seed = "seed"
    discovered = "discovered"
    classified = "classified"
    matched = "matched"
    ready_for_activation = "ready_for_activation"
    active = "active"
    paused = "paused"
    rejected = "rejected"
    error = "error"


class ActivationMode(str, Enum):
    manual = "manual"
    automatic = "automatic"


class JobRunStatus(str, Enum):
    running = "running"
    success = "success"
    failed = "failed"
    skipped = "skipped"


class SourceParseStatus(str, Enum):
    running = "running"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"


class SourceFrontierStatus(str, Enum):
    new = "new"
    processing = "processing"
    processed = "processed"
    error = "error"


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
