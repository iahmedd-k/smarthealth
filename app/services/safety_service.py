import re
from dataclasses import dataclass

from app.core.exceptions import ValidationError


@dataclass(frozen=True)
class SafetyDecision:
    intent: str
    refused: bool
    acute: bool = False
    hard_blocked: bool = False


EMERGENCY_MESSAGE = (
    "This sounds like it could be a medical emergency. Please call your local "
    "emergency number or go to the nearest emergency room right now - "
    "I'm not able to help with this here."
)


class SafetyCheck:
    """Conservative, deterministic gate that runs before embeddings or retrieval."""

    _common_typos = {
        "appoietmente": "appointment",
        "appoietments": "appointments",
        "appoietment": "appointment",
        "appoietmens": "appointments",
        "appoitment": "appointment",
        "appoitments": "appointments",
        "appoitmens": "appointments",
        "availabel": "available",
        "serivce": "service",
        "serivces": "services",
    }

    _medical = re.compile(
        r"\b(diagnos|cause|caused|symptom|fever|temperature|pain|ache|burn\w*|burin\w*|"
        r"treat|treatment|medication|medicine|prescri|dose|dosage|what do i have|"
        r"what's wrong with me|prescribe)\w*\b",
        re.I,
    )
    _acute = re.compile(
        r"\b(?:heart|chest)\b.{0,30}\b(?:pain|ache|burn\w*|burin\w*)\b"
        r"|\b(?:severe|unbearable|intense)\s+(?:pain|ache)\b"
        r"|\b(?:chest pain|heart pain|heart ache|difficulty breathing|can't breathe|cannot breathe|"
        r"stroke|unconscious|severe bleeding|overdose|suicid\w*|self[- ]harm|hurt(?:ing)? myself|"
        r"kill myself|end my life|don'?t want to (?:be here|live) anymore)\b",
        re.I,
    )
    _preparation = re.compile(r"\b(prepare|preparation|bring|fast|fasting|arrive|metal|instructions)\w*\b", re.I)
    _availability = re.compile(r"\b(available|availability|avialbe|avialability|open slot|open slots|slot|slots|when can i)\w*\b", re.I)
    _appointment_word = r"app(?:\w{0,10}ment\w*|oit\w{0,8})"
    _booking = re.compile(
        r"\b(how (?:can|do) i book|book an appointment|make an appointment|schedule an appointment|"
        r"how to book|i want to book|book that slot|reserve that slot)\b",
        re.I,
    )
    _cancellation = re.compile(
        rf"\b(cancel|cancellation|reschedule|change)\b.{{0,40}}\b(?:my|the)?\s*{_appointment_word}\b"
        rf"|\b{_appointment_word}\b.{{0,40}}\b(cancel|cancellation|reschedule|change)\b",
        re.I,
    )
    _appointment = re.compile(
        rf"\b(my|me|for me)\b.{{0,60}}\b{_appointment_word}\b"
        rf"|\b{_appointment_word}\b.{{0,60}}\b(booked|reserved|status|today)\b"
        rf"|\b(booked|reserved)\b.{{0,30}}\b{_appointment_word}\b"
        rf"|\b(current appointment|booked slots?|slots? (?:i have )?booked|what .*slots? .*booked|do i have .*appointments?|reschedule my|cancel my|when is my|check my appointment|appointment status)\b",
        re.I,
    )
    _specialist_navigation = re.compile(r"\b(which|what|who)\b.{0,40}\b(specialist|doctor|department|service)\b|\bwho should i see\b", re.I)
    _gibberish = re.compile(r"^(?:[bcdfghjklmnpqrstvwxyz]{6,}|[a-z]{1,2}\d{3,}|(?:\W|_)+)$", re.I)

    def normalize(self, question: str) -> str:
        normalized = " ".join(question.split())
        if not normalized:
            raise ValidationError("Question cannot be empty", code="QUESTION_EMPTY")
        if len(normalized) > 2000:
            raise ValidationError("Question is too long", code="QUESTION_TOO_LONG")
        if self._looks_gibberish(normalized):
            raise ValidationError("Question looks invalid", code="QUESTION_INVALID")
        return re.sub(
            r"\b[A-Za-z]+\b",
            lambda match: self._common_typos.get(match.group(0).lower(), match.group(0)),
            normalized,
        )

    def _looks_gibberish(self, question: str) -> bool:
        compact = re.sub(r"\s+", "", question)
        if not compact:
            return True
        if question.casefold().strip() in {"hi", "hello", "hey", "help", "thanks", "thank you", "how are you"}:
            return False

        alpha_count = sum(1 for char in compact if char.isalpha())
        if alpha_count < 3:
            return True

        # Vowel-based heuristics only apply to strings that are mostly
        # Latin-script letters, so Urdu/Arabic/CJK/etc. input (which has
        # no a/e/i/o/u by definition) isn't wrongly flagged as gibberish.
        latin_alpha = sum(1 for char in compact if char.isascii() and char.isalpha())
        is_mostly_latin = alpha_count > 0 and latin_alpha >= alpha_count * 0.8

        if is_mostly_latin:
            vowel_count = sum(1 for char in compact.lower() if char in "aeiou")
            if alpha_count >= 6 and vowel_count == 0:
                return True

        if len(compact) >= 12 and len(set(compact.lower())) <= 3:
            return True
        if self._gibberish.match(question):
            return True

        alnum_count = sum(1 for char in compact if char.isalnum())
        if alnum_count and (alpha_count / alnum_count) < 0.3:
            return True

        # NOTE: intentionally no vowel-ratio-under-0.22 check here. That
        # heuristic rejected real short phrases like "list down my slots"
        # (3 vowels / 15 letters = 0.20) as invalid. The checks above
        # already catch genuinely low-information / repeated-noise input
        # without that false-positive risk.
        return False

    def check_emergency(self, question: str) -> SafetyDecision:
        """Hard, non-overridable gate. Call this BEFORE retrieval or the LLM.
        If hard_blocked is True, return EMERGENCY_MESSAGE directly - do not
        call the model at all."""
        if self._acute.search(question):
            return SafetyDecision(
                intent="acute_medical_advice",
                refused=True,
                acute=True,
                hard_blocked=True,
            )
        return SafetyDecision(intent="navigation", refused=False)

    def classify(self, question: str) -> SafetyDecision:
        emergency = self.check_emergency(question)
        if emergency.hard_blocked:
            return emergency

        # Medical content is checked BEFORE specialist_navigation on purpose:
        # a question like "which specialist treats my fever medication
        # needs?" matches both patterns, and must be refused as medical
        # advice rather than slipping through as plain navigation just
        # because it also contains a routing phrase.
        if self._medical.search(question):
            return SafetyDecision("medical_advice", True)

        if self._specialist_navigation.search(question):
            return SafetyDecision("specialist_navigation", False)
        if self._cancellation.search(question):
            return SafetyDecision("cancellation", False)
        if self._preparation.search(question):
            return SafetyDecision("preparation", False)
        if self._appointment.search(question):
            return SafetyDecision("appointment", False)
        if self._booking.search(question):
            return SafetyDecision("booking", False)
        if self._availability.search(question):
            return SafetyDecision("availability", False)
        return SafetyDecision("navigation", False)