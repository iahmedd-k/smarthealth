"""Create demo login users and their required role profiles."""

from app.core.security import get_password_hash
from app.db import SessionLocal
from app.models import User, UserRole
from app.repositories import AuthRepository, PatientRepository, ProviderRepository


DEMO_USERS = (
    ("admin@example.com", "secret123", UserRole.admin),
    ("provider@example.com", "secret123", UserRole.provider),
    ("patient@example.com", "secret123", UserRole.patient),
    ("frontdesk@example.com", "secret123", UserRole.front_desk),
    ("demo@gmail.com", "adminadmin", UserRole.admin),
)


def seed_users() -> None:
    db = SessionLocal()
    repository = AuthRepository(db)
    patients = PatientRepository(db)
    providers = ProviderRepository(db)
    try:
        for email, password, role in DEMO_USERS:
            user = repository.get_user_by_email(email)
            if user is None:
                user = User(email=email, is_active=True)
                db.add(user)
            user.hashed_password = get_password_hash(password)
            user.role = role
            user.is_active = True
        db.commit()

        patient_user = repository.get_user_by_email("patient@example.com")
        if patient_user and not patients.get_by_user_id(patient_user.id):
            patients.create_seed_profile(patient_user.id, "Pat", "Patient")

        provider_user = repository.get_user_by_email("provider@example.com")
        if provider_user and not providers.get_by_user_id(provider_user.id):
            providers.create_seed_provider(provider_user.id, None, "Cardiology specialist")
    finally:
        db.close()


if __name__ == "__main__":
    seed_users()