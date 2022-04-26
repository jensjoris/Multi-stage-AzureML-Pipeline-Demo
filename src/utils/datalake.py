from azureml.core.dataset import Dataset


class DatasetFilter:
    """
    Utility class to apply logical filters on top of a partitioned Azure Dataset.
    """
    def __init__(self, dataset, countries=None, years=None, months=None) -> None:
        self._filter = None
        self.dataset = dataset

        country_filter = self._in("dirCountry", countries or [])
        year_filter = self._in("dirYear", years or [])
        month_filter = self._in("dirMonth", months or [])

        self._filter = self._and([country_filter, year_filter, month_filter])
    
    def get_filter(self):
        return self._filter
    
    def _in(self, column, values):
        _filter = None
        for value in values:
            if _filter is None:
                _filter = self.dataset[column] == value
            else:
                _filter = _filter | (self.dataset[column] == value)
        return _filter
    
    def _and(self, filters):
        _filter = None
        for filter in filters:
            if filter is None:
                continue
            if _filter is None:
                _filter = filter
            else:
                _filter = _filter & filter
        return _filter


def get_vacancies(ws, location, countries=None, years=None, months=None):
    blob_datastore = ws.get_default_datastore()

    # The partition format will add logical columns `dirCountry`, 
    # `dirYear`, `dirMonth` and `dirFilename` to the Azure Dataset. 
    file_dataset = Dataset.File.from_files(
        path=(blob_datastore, location),
        partition_format="country={dirCountry}/year={dirYear}/month={dirMonth}/{dirFilename}.parquet"
    )

    # Apply the filters onto the dataset.
    _filter = DatasetFilter(file_dataset, countries=countries, years=years, months=months).get_filter()
    if _filter is not None:
        file_dataset = file_dataset.filter(_filter)

    return file_dataset


def get_already_processed_vacancies(ws, location="/dataset/enriched-vacancies"):
    blob_datastore = ws.get_default_datastore()
    try:
        return Dataset.File.from_files(
            path=(blob_datastore, location)
        )
    except:
        return None
