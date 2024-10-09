from sqlalchemy import create_engine, ForeignKey, Table, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from typing import Optional

db_url = "sqlite:///database.db"

engine = create_engine(db_url, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)


document_type_custom_field_association = Table(
    "document_type_custom_field",
    Base.metadata,
    Column(
        "document_type_id",
        ForeignKey("document_types.id"),
    ),
    Column(
        "custom_field_id",
        ForeignKey("custom_fields.id"),
    ),
)

class Document(Base):
    __tablename__ = "documents"

    name: Mapped[str]
    document_type: Mapped["DocumentType"] = relationship(
        primaryjoin="DocumentType.id == Document.document_type_id",
    )
    document_type_id: Mapped[Optional[int]] = mapped_column(ForeignKey("document_types.id"))


class CustomField(Base):
    __tablename__ = "custom_fields"
    name: Mapped[str]
    data_type: Mapped[str]


class CustomFieldValue(Base):
    __tablename__ = "custom_field_values"
    document_id: Mapped[int] = mapped_column(
        ForeignKey("documents.id")
    )
    field_id: Mapped[int] = mapped_column(ForeignKey("custom_fields.id"))
    field = relationship(
        "CustomField", primaryjoin="CustomField.id == CustomFieldValue.field_id"
    )
    value: Mapped[Optional[str]]


class DocumentType(Base):
    __tablename__ = "document_types"
    name: Mapped[str]
    custom_fields: Mapped[list["CustomField"]] = relationship(
        secondary=document_type_custom_field_association
    )

