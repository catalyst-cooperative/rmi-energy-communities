"""Module extracting the raw data from their source. 

Each module in this subpackage implements the extraction from a single data source. 
This process ends with a dictionary of :class:`pandas.DataFrame` objects that are
minimally altered from their original data and are ready for cleaning in the transform modules."""