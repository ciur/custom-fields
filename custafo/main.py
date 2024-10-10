import random

import sqlalchemy
import typer
from sqlalchemy import select, text, literal_column, and_, or_, func, cast, desc
from sqlalchemy.orm import join, aliased
from custafo.schema import Base, engine, Document, DocumentType, CustomField, \
    CustomFieldValue, session, DocumentTypeCustomField

app = typer.Typer()



@app.command()
def create():
    Base.metadata.create_all(engine)


@app.command()
def drop():
    Base.metadata.drop_all(engine)


@app.command()
def insert_document_types():
    session.add_all([
        DocumentType(name="invoice"),
        DocumentType(name="bill"),
        DocumentType(name="contract")
    ])
    session.commit()

@app.command()
def ins_docs():
    cf_date = CustomField(
        name="date",
        data_type="date"
    )
    cf_total = CustomField(
        name="total",
        data_type="monetary"
    )
    cf_name = CustomField(
        name="name",
        data_type="string"
    )
    invoice = DocumentType(name="invoice", custom_fields=[cf_date, cf_total, cf_name])
    bill = DocumentType(name="bill", custom_fields=[cf_date, cf_name])
    contract = DocumentType(name="contract", custom_fields=[cf_date])

    session.add_all([invoice, bill, contract])

    for num in range(1, 100):
        doc = Document(name=f"doc_{num}.pdf", document_type=invoice)
        session.add(doc)

    for num in range(101, 200):
        doc = Document(name=f"doc_{num}.pdf", document_type=bill)
        session.add(doc)

    for num in range(201, 300):
        doc = Document(name=f"doc_{num}.pdf", document_type=contract)
        session.add(doc)

    session.commit()

@app.command()
def ins_cf_values():
    for doc_id in range(1, 50):
        year = random.choice([2024, 2023, 2022, 2021, 2022])
        month =  random.choice(range(1, 12))
        day = random.choice(range(1, 30))
        cfv1 = CustomFieldValue(
            document_id=doc_id,
            field_id=1,  # date
            value=f"{day}.{month}.{year}"
        )
        cfv2 = CustomFieldValue(
            document_id=doc_id,
            field_id=2,  # total
            value=f"{random.choice(range(1, 100))}"
        )
        cfv3 = CustomFieldValue(
            document_id=doc_id,
            field_id=3,  # date
            value=random.choice(["lidl", "rewe", "aldi"])
        )
        session.add_all([cfv1, cfv2, cfv3])
    session.commit()


@app.command()
def list_docs1():
    result = session.execute(text("select id, name from documents"))
    for row in result:
        print(f"{row.id} {row.name}")

@app.command()
def play1():
    stmt = select(
        Document.id.label("DOC_ID"),
        Document.name.label("DOC_NAME")
    ).where(Document.id == 1)
    print(stmt)
    for row in session.execute(stmt):
        print(f"{row.DOC_ID} {row.DOC_NAME}")


@app.command()
def play2():
    stmt = select(
        literal_column("'something here'").label('p'),
        Document.id.label("DOC_ID"),
        Document.name.label("DOC_NAME")
    ).where(Document.id == 1)
    print(stmt)
    for row in session.execute(stmt):
        print(f"{row.p} {row.DOC_ID} {row.DOC_NAME}")


@app.command()
def play3():
    stmt = select(
        Document.id.label("DOC_ID"),
        Document.name.label("DOC_NAME")
    ).where(
        or_(
            Document.id == 1,
            Document.id == 2
        )
    )
    print(stmt)
    for row in session.execute(stmt):
        print(f"{row.DOC_ID} {row.DOC_NAME}")


@app.command()
def playj1():
    stmt = select(Document).join(DocumentType).where(
        DocumentType.name=="invoice", Document.id < 5
    )
    for doc in session.scalars(stmt):
        print(f"{doc} {doc.document_type.name}")


@app.command()
def playj2():
    stmt = select(Document).join_from(Document, DocumentType).where(
        DocumentType.name=="invoice", Document.id < 5
    )
    print(stmt)
    #for doc in session.scalars(stmt):
    #    print(f"{doc} {doc.document_type.name}")

@app.command()
def playj3():
    dtype_1 = aliased(DocumentType)
    stmt = select(
        Document.id,
        Document.name,
        dtype_1.id,
        dtype_1.name,
        CustomField.id,
        CustomField.name
    ).join(dtype_1).join_from(
        dtype_1,
        DocumentTypeCustomField,
        DocumentTypeCustomField.document_type_id == dtype_1.id
    ).join_from(
        DocumentTypeCustomField,
        CustomField,
        DocumentTypeCustomField.custom_field_id == CustomField.id
    ).where(
        DocumentType.name == "invoice", Document.id < 5
    )
    print(stmt)
    for row in session.execute(stmt):
        print(row)


def get_subq():
    cfv = aliased(CustomFieldValue)
    cf = aliased(CustomField)
    dtcf = aliased(DocumentTypeCustomField)
    dt = aliased(DocumentType)
    doc = aliased(Document)

    subq = (
        select(
            doc.id.label("doc_id"),
            doc.name.label("doc_name"),
            dt.name.label("document_type"),
            dt.id.label("document_type_id"),
            cf.name.label("cf_name"),
            cast(cfv.value, sqlalchemy.Integer).label("cf_value")
        ).select_from(cfv).join(
            cf, cf.id == cfv.field_id
        ).join(
            dtcf, dtcf.custom_field_id == cf.id
        ).join(dt, dt.id == dtcf.document_type_id).join(
            doc, doc.document_type_id == dt.id
        ).where(
            dt.name == "invoice",
            cf.name == "total",
            doc.id == cfv.document_id,
            cast(cfv.value, sqlalchemy.Integer) > 10,
            cast(cfv.value, sqlalchemy.Integer) < 30,
        ).subquery()
    )

    return subq


@app.command()
def playj4():
    subq = get_subq()
    stmt = select(
        subq.c.doc_id,
        subq.c.doc_name,
        subq.c.document_type,
        subq.c.cf_name,
        subq.c.cf_value
    ).select_from(subq).order_by(
        desc(subq.c.cf_value)
    )

    for row in session.execute(stmt):
        print(row)


@app.command()
def playj5():
    cfv = aliased(CustomFieldValue)
    cf = aliased(CustomField)
    dtcf = aliased(DocumentTypeCustomField)
    dt = aliased(DocumentType)

    subq = get_subq()
    stmt = select(
        subq.c.doc_id.label("doc_id"),
        subq.c.doc_name.label("doc_name"),
        cf.name.label("cf_name"),
        cfv.value.label("cf_value")
    ).select_from(cfv).join(
       cf, cf.id == cfv.field_id
    ).join(
        dtcf, dtcf.custom_field_id == cf.id
    ).join(
        dt, dt.id == dtcf.document_type_id
    ).join(subq, subq.c.document_type_id == dt.id).where(
        subq.c.doc_id == cfv.document_id
    ).order_by(subq.c.cf_value)

    documents = {}
    for row in session.execute(stmt):
        if row.doc_id not in documents.keys():
            documents[row.doc_id] = {
                "doc_id": row.doc_id,
                "doc_name": row.doc_name,
                "custom_fields": []
            }
        else:
            documents[row.doc_id]["custom_fields"].append(
                (row.cf_name, row.cf_value)
            )

    for key, value in documents.items():
        print(f"{value['doc_id']} {value['doc_name']} {value['custom_fields']}")

