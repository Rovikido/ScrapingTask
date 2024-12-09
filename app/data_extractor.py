class ContractorDataExtractor:
    def __init__(self, fields_to_extract=None, format_location=True):
        """
        Initializes the contractor data extractor with customizable fields and location formatting option.

        Args:
            fields_to_extract (dict): A dictionary specifying which fields to extract.
                                      Keys are 'contractor_fields', 'location_fields', and 'contractor_user_fields'.
                                      Each value should be a list of field names to extract.
                                      Example:
                                      fields_to_extract = {
                                        "contractor_fields": ["professionalId", "formattedPhone", "budgetLevels", "numReviews"],
                                        "location_fields": ["location", "address", "city", "state", "zip", "country", "latitude", "longitude"],
                                        "contractor_user_fields": ["displayName", "houzzLink", "socialLinks"]
                                    }
            format_location (bool): Flag to control whether location data should be formatted (default is True).
        """
        # There is a way to make this completely customizable, with specialization for which group of fields to get and from which path, but would be a bit of a overengeniering 
        self.store_data_path = ["ctx", "data", "stores", "data", "ProfessionalStore", "data"]
        self.contractor_user_data_path = ["ctx", "data", "stores", "data", "UserStore", "data"]

        self.contractor_fields = fields_to_extract.get("contractor_fields")
        self.location_fields = fields_to_extract.get("location_fields")
        self.contractor_user_fields = fields_to_extract.get("contractor_user_fields")

        self.format_location = format_location

    def extract_fields(self, data_entry, fields):
        """Helper method to extract fields from a contractor's data."""
        extracted = {}
        for field in fields:
            extracted[field] = data_entry.get(field)
        return extracted

    def format_location_data(self, location_data):
        """Formats location-related data into a nested structure using the location fields."""
        formatted_location = {}
        for field in self.location_fields:
            formatted_location[field] = location_data.get(field)
        return formatted_location

    def extract_data(self, data):
        """
        Extracts contractor and user data from the given nested dictionary.

        Args:
            data (dict): The input data containing contractor and user information.

        Returns:
            dict: A dictionary where each contractor ID maps to its extracted and structured data.
        """
        store_data = self._get_data_from_path(data, self.store_data_path)
        contractor_user_data = self._get_data_from_path(data, self.contractor_user_data_path)

        contractors = {}
        for contractor_id, professional_data in store_data.items():
            # Extract basic contractor fields
            basic_data = self.extract_fields(professional_data, self.contractor_fields)
            
            # Extract location data (conditionally format if specified)
            location_data = self.extract_fields(professional_data, self.location_fields)
            if self.format_location:
                location_data = self.format_location_data(location_data)

            # Extract user data
            user_data = self.extract_fields(contractor_user_data.get(contractor_id, {}), self.contractor_user_fields)
            
            # Combine all data
            contractors[contractor_id] = {
                **user_data,
                **basic_data,
                "locationData": location_data,
            }

        return contractors

    def _get_data_from_path(self, data, path):
        """Utility method to navigate through nested data using a path."""
        for key in path:
            data = data.get(key, {})
        return data
