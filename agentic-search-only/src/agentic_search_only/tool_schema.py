"""Chat-completions function-tool schema builder."""
def function(name, description, properties, required):
    return dict(type='function', function=dict(name=name, description=description,
                parameters=dict(type='object', properties=properties, required=required, additionalProperties=False)))
