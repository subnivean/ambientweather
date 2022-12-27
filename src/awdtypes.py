def get_awdtypes(lastdata):
    """Creates a mapping of AmbientWeather station properties to their data types.

    Used for creating a Sqlite table to store the data.

    Args:
        lastdata: Output of `AmbientAPI().get_devices()[<AWDeviceNumber>].last_data`.

    Returns:
        Dictionary mapping AmbientWeather station properties to their data types.
    """

    STRFIELDS = ("lastRain", "tz", "date")

    dtypes = eval(
        "dict("
        + ",\n".join(
            [
                f"{key}={'str' if key in STRFIELDS else 'float'}"
                for key in lastdata.keys()
            ]
        )
        + ")"
    )

    return dtypes
