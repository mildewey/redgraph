import os

import graphql

with open(os.path.join(os.path.dirname(__file__), "schema.gql")) as f:
    schema = graphql.build_schema(f.read())
