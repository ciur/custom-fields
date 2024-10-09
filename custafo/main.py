import typer

from custafo.schema import Base, engine

app = typer.Typer()



@app.command()
def create():
    Base.metadata.create_all(engine)


@app.command()
def drop():
    Base.metadata.drop_all(engine)

