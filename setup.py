from distutils.core import setup

setup(
    name="redgraph",
    version="0.0.0",
    description="Python interface for a graph database backed by redis.",
    author="Mark Dewey",
    author_email="mildewey@gmail.com",
    url="https://github.com/mildewey/redgraph",
    packages=["redgraph"],
    package_dir={"": "src"},
    install_requires=["hiredis", "aioredis", "pydantic",],
)
