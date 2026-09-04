import asyncio
import json
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

import httpx

from app.core.exceptions import ExternalServiceError
from app.core.settings import settings


class LLMProvider(ABC):
    @abstractmethod
    async def stream(self, prompt: str) -> AsyncIterator[str]:
        raise NotImplementedError

    @abstractmethod
    async def complete(self, prompt: str) -> str:
        raise NotImplementedError

    @abstractmethod
    async def complete_json(self, prompt: str) -> str:
        raise NotImplementedError


class FakeLLM(LLMProvider):
    """Deterministic provider for tests and local development."""

    def __init__(self, answer: str | None = None) -> None:
        self.answer = answer

    def _answer_from_prompt(self, prompt: str) -> str:
        """Build a grounded navigation reply from the catalog context in the prompt."""
        import re

        match = re.search(
            r"\[([^\]]+)\]\s*([^.]+)\.\s*(.*?)(?:Available appointments:\s*(.+?))?(?:\n---|\n\nPatient question|$)",
            prompt,
            flags=re.S,
        )
        if not match:
            return (
                "Based on the available clinic services, please choose the service that best "
                "matches your appointment needs."
            )

        department = match.group(1).strip()
        service_name = " ".join(match.group(2).split())
        content = " ".join((match.group(3) or "").split()).strip()
        slots = " ".join((match.group(4) or "").split()).strip().rstrip(".")

        answer = f"Yes — we offer {service_name} in {department}."
        if content:
            answer += f" {content.rstrip('.')}."
        if slots and slots.lower() != "no available slots currently":
            answer += f" Next openings include: {slots}."
        elif slots:
            answer += " There are no available appointment slots for this service right now."
        return answer

    async def stream(self, prompt: str) -> AsyncIterator[str]:
        answer = self.answer or self._answer_from_prompt(prompt)
        for token in answer.split():
            yield f"{token} "

    async def complete(self, prompt: str) -> str:
        if self.answer:
            return self.answer
        lowered = prompt.lower()
        if "follow-up" in lowered or "follow up" in lowered:
            return (
                "Subject: Thank you for your visit\n\n"
                "Body:\n"
                "Thank you for visiting our clinic. We appreciate your time today.\n\n"
                "Next steps:\n"
                "- Contact us if you have any questions\n"
                "- Schedule a follow-up appointment if recommended\n"
            )
        return (
            "Your appointment is confirmed.\n\n"
            "Instructions:\n"
            "Please arrive 10 minutes early and bring your ID.\n\n"
            "Cancellation:\n"
            "24-hour notice is required to cancel or reschedule."
        )

    async def complete_json(self, prompt: str) -> str:
        return (
            '{"period_start":"1970-01-01","period_end":"1970-01-01",'
            '"appointments_booked":0,"completed_visits":0,"cancellations":0,'
            '"total_patients":0,"failed_workflows":0}'
        )


class OpenAICompatibleLLM(LLMProvider):
    def __init__(self, base_url: str, api_key: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    async def _complete(self, prompt: str, stream: bool = False) -> Any:
        max_retries = 2
        timeout = httpx.Timeout(connect=3.0, read=settings.llm_timeout_seconds, write=3.0, pool=3.0)

        for attempt in range(1, max_retries + 2):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(
                        f"{self.base_url}/chat/completions",
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={"model": self.model, "messages": [{"role": "user", "content": prompt}], "stream": stream},
                    )
                    if response.status_code >= 500 and attempt <= max_retries:
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    response.raise_for_status()
                    return response
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
                if attempt <= max_retries:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                raise ExternalServiceError(
                    "LLM provider is temporarily unavailable; please try again later.",
                    status_code=502,
                    code="LLM_UNAVAILABLE",
                ) from exc

        raise ExternalServiceError(
            "LLM provider is temporarily unavailable; please try again later.",
            status_code=502,
            code="LLM_UNAVAILABLE",
        )

    async def stream(self, prompt: str) -> AsyncIterator[str]:
        timeout = httpx.Timeout(connect=3.0, read=settings.llm_timeout_seconds, write=3.0, pool=3.0)
        for attempt in range(1, 4):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "POST",
                        f"{self.base_url}/chat/completions",
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={"model": self.model, "messages": [{"role": "user", "content": prompt}], "stream": True},
                    ) as response:
                        response.raise_for_status()
                        async for line in response.aiter_lines():
                            if not line.startswith("data:"):
                                continue
                            payload = line[5:].strip()
                            if payload == "[DONE]":
                                return
                            try:
                                content = json.loads(payload)["choices"][0]["delta"].get("content", "")
                            except (KeyError, IndexError, TypeError, ValueError) as exc:
                                raise ExternalServiceError("LLM provider returned an invalid streaming response", status_code=502, code="LLM_INVALID_RESPONSE") from exc
                            if content:
                                yield content
                return
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
                if attempt < 3:
                    await asyncio.sleep(0.5 * attempt)
                    continue
                raise ExternalServiceError(
                    "LLM provider is temporarily unavailable; please try again later.",
                    status_code=502,
                    code="LLM_UNAVAILABLE",
                ) from exc

    async def complete(self, prompt: str) -> str:
        response = await self._complete(prompt, stream=False)
        try:
            return response.json()["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ExternalServiceError("LLM provider returned an invalid response", status_code=502, code="LLM_INVALID_RESPONSE") from exc

    async def complete_json(self, prompt: str) -> str:
        return await self.complete(prompt)


def get_llm_provider() -> LLMProvider:
    if not settings.llm_api_key:
        if settings.use_fake_llm:
            return FakeLLM()
        raise ExternalServiceError(
            "LLM provider is not configured; set LLM_API_KEY or explicitly enable USE_FAKE_LLM.",
            status_code=503,
            code="LLM_NOT_CONFIGURED",
        )
    base_url = settings.llm_base_url
    if settings.llm_provider.lower() == "groq" and base_url == "https://api.openai.com/v1":
        base_url = "https://api.groq.com/openai/v1"
    return OpenAICompatibleLLM(base_url, settings.llm_api_key, settings.llm_model)
