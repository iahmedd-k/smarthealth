import asyncio
from types import SimpleNamespace

import pytest

from app.core.settings import Settings
from app.core.authorization.permissions import Permission, ROLE_PERMISSIONS
from app.services.safety_service import EMERGENCY_MESSAGE, SafetyCheck
from app.services.assistant_service import AssistantService
from app.services.assistant_prompts import DISCLAIMER


def test_production_requires_llm_credentials():
    with pytest.raises(ValueError, match="LLM_API_KEY must be set"):
        Settings(
            app_env="production",
            jwt_secret="a" * 48,
            llm_provider="groq",
            llm_api_key="",
        )


def test_unknown_llm_provider_is_rejected():
    with pytest.raises(ValueError, match="Unsupported LLM provider"):
        Settings(app_env="test", llm_provider="not-a-provider")


def test_ai_role_permissions_match_business_boundaries():
    from app.models import UserRole

    assert Permission.ANALYTICS_READ not in ROLE_PERMISSIONS[UserRole.patient]
    assert Permission.ANALYTICS_READ not in ROLE_PERMISSIONS[UserRole.provider]
    assert Permission.ANALYTICS_READ in ROLE_PERMISSIONS[UserRole.front_desk]
    assert Permission.ANALYTICS_READ in ROLE_PERMISSIONS[UserRole.admin]
    assert Permission.APPOINTMENT_CREATE in ROLE_PERMISSIONS[UserRole.patient]


def test_acute_heart_burning_is_provider_independent():
    decision = SafetyCheck().classify("my heart is burining what should i do")

    assert decision.refused is True
    assert decision.acute is True
    assert decision.hard_blocked is True
    assert decision.intent == "acute_medical_advice"
    assert EMERGENCY_MESSAGE.startswith("This sounds like it could be a medical emergency.")


def test_emergency_gate_catches_severe_pain_and_self_harm():
    checker = SafetyCheck()

    assert checker.check_emergency("I have severe pain").hard_blocked is True
    assert checker.check_emergency("I might hurt myself").hard_blocked is True


def test_misspelled_own_appointment_questions_reach_account_lookup():
    checker = SafetyCheck()

    assert checker.classify("do i have today any appoietmente booked reserverd for me").intent == "appointment"
    assert checker.classify("list down my booked appoietments").intent == "appointment"
    assert checker.classify("what are my booked appoitmens").intent == "appointment"
    assert checker.normalize("what are my booked appoitmens") == "what are my booked appointments"


def test_common_misspellings_are_normalized_before_intent_routing():
    checker = SafetyCheck()

    assert checker.classify("list down availabel slots").intent == "availability"
    assert checker.normalize("any serivces you offered") == "any services you offered"


def test_logistics_intents_are_not_collapsed():
    checker = SafetyCheck()

    assert checker.classify("do you have a slot for opd").intent == "availability"
    assert checker.classify("i want to book that slot").intent == "booking"
    assert checker.classify("cancel my appointment").intent == "cancellation"
    assert checker.classify("what should i bring for my appointment").intent == "preparation"
    assert checker.classify("what are my current appointments").intent == "appointment"


def test_service_listing_is_exact_catalog_text():
    service = AssistantService.__new__(AssistantService)
    answer = service._format_service_listing([
        {"service_id": 1, "service_name": "Heart Checkup ", "department": "Cardiology"},
        {"service_id": 2, "service_name": "Heart Checkup", "department": "Cardiology"},
    ])

    assert answer == "Available services: Heart Checkup (Cardiology)."
    assert "Preparation" not in answer
    assert "Specialty" not in answer
    assert "**" not in answer


def test_offer_heart_consultation_is_not_a_generic_listing():
    service = AssistantService.__new__(AssistantService)
    question = "i want kwno do you offer heart conslulatins explain please"

    assert service._is_service_listing_question(question) is False
    assert service._is_service_listing_question("what services do you offer") is True


def test_clinic_overview_and_mixed_logistics_are_detected():
    service = AssistantService.__new__(AssistantService)

    assert service._is_service_listing_question("tell me about your clinic") is True
    assert service._is_service_listing_question("what kind of services do you people offer") is True
    assert service._is_pricing_question("what are the charges?") is True
    assert service._has_safe_logistics_request("I want heart treatment and available slots tomorrow") is True


def test_availability_uses_prior_retrieved_service_ids(monkeypatch):
    service = AssistantService.__new__(AssistantService)
    service.db = None
    service.services = SimpleNamespace(
        get_by_id=lambda service_id: SimpleNamespace(
            id=service_id,
            name="General Consultation",
            is_published=True,
            department=SimpleNamespace(name="Cardiology"),
            specialty="Cardiology",
            description="Heart consultation",
        )
    )
    service.slots = SimpleNamespace(
        list_by_service=lambda *args, **kwargs: (
            [
                SimpleNamespace(
                    start_datetime=__import__("datetime").datetime(2026, 9, 10, 9, 0, tzinfo=__import__("datetime").timezone.utc)
                )
            ],
            1,
        )
    )
    monkeypatch.setattr(
        "app.services.assistant_service.search_services",
        lambda *args, **kwargs: asyncio.sleep(0, result=[]),
    )

    answer, citations, retrieved_ids = asyncio.run(
        service._answer_availability("also any slots avialble", fallback_service_ids=[7])
    )

    assert "General Consultation" in answer
    assert "available slot" in answer.lower()
    assert retrieved_ids == [7]
    assert citations[0]["service_id"] == 7


def test_specialist_navigation_uses_real_service_preparation(monkeypatch):
    service = AssistantService.__new__(AssistantService)
    service.db = None
    service.services = SimpleNamespace(
        get_by_id=lambda service_id: SimpleNamespace(
            id=service_id,
            name="Orthopaedics Consultation",
            preparation_instructions="Bring prior imaging reports",
        )
    )
    monkeypatch.setattr(
        "app.services.assistant_service.search_services",
        lambda *args, **kwargs: asyncio.sleep(0, result=[
            {
                "service_id": 42,
                "service_name": "Orthopaedics Consultation",
                "department": "Orthopaedics",
            }
        ]),
    )

    answer, citations, retrieved_ids = asyncio.run(
        service._answer_specialist_navigation("Which specialist should I see for knee pain?")
    )

    assert "Orthopaedics Consultation" in answer
    assert "Bring prior imaging reports." in answer
    assert DISCLAIMER in answer
    assert citations == [{"service_id": 42, "service_name": "Orthopaedics Consultation", "department": "Orthopaedics"}]
    assert retrieved_ids == [42]
