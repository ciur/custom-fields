import typer
from sqlalchemy import select
from sqlalchemy.orm import join
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
def list_docs():
    #stmt = select(Document).join(
    #    DocumentType
    #).join(
    #    CustomField
    #)
    #docs = session.scalar(stmt).all()
    #for doc in docs:
    #    print(f"|{doc.id}|{doc.document_type.name}")
    #stmt = select(Document).join(DocumentType).filter(
    #    Document=="invoice"
    #)

    docs = session.scalars(stmt).all()
    for doc in docs:
        cf_names = "|".join([cf.name for cf in doc.document_type.custom_fields])
        print(f"{doc.name}|{doc.document_type.name}|{cf_names}")

