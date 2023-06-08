def type_validation(pydict_map: dict, pydict: dict) -> None:
    for key, value in pydict_map.items():
        if key.endswith("?"):
            if key[:-1] in pydict:
                if pydict[key[:-1]] is not None:
                    if not isinstance(pydict[key[:-1]], value):
                        raise TypeError(
                            f"Invalid type for {key[:-1]}: {type(pydict[key[:-1]])}, expected {value}"
                        )
        else:
            if not isinstance(pydict[key], value):
                raise TypeError(
                    f"Invalid type for {key}: {type(pydict[key])}, expected {value}"
                )
