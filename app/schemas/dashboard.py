from pydantic import BaseModel


class DashboardSummary(BaseModel):
    parser_total: int
    parser_new: int
    parser_possible_duplicate: int
    parser_duplicate: int
    leads_total: int
    deals_total: int
    leads_by_status: dict[str, int]
    deals_by_status: dict[str, int]
    conversion_lead_to_deal_percent: float
    pipeline_value_rub: float

