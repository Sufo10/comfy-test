import json

class JSONArrayPathMultilineConcatenator:
    """
    A custom node to iterate over a JSON array, extract a value from each object 
    using a dot-separated path, and concatenate the results into a multiline string.
    The delimiter is hardcoded to a newline ('\n').
    """
    
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_array_string": ("STRING", {"multiline": True, "default": "[{\"a\": {\"b\": \"Value1\"}}, {\"a\": {\"b\": \"Value2\"}}]","tooltip": "JSON string containing an array of objects."}),
                "path_string": ("STRING", {"default": "a.b", "tooltip": "Dot-separated path to the nested value (e.g., 'a.b.c')."}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("concatenated_multiline_string",)
    FUNCTION = "concat_by_path"
    CATEGORY = "Utils/JSON"

    def _get_nested_value(self, data, path_parts, placeholder):
        """
        Safely retrieves a value from a nested dictionary using a list of keys.
        Returns the placeholder if any key along the path is missing.
        """
        current_data = data
        
        for key in path_parts:
            # Check if the current data is a dictionary and contains the key
            if isinstance(current_data, dict) and key in current_data:
                current_data = current_data[key]
            else:
                # Path segment not found, return the placeholder
                return placeholder
        
        # Successfully found the value, return it as a string
        return str(current_data)


    # Removed 'delimiter' and 'not_found_placeholder' from inputs
    def concat_by_path(self, json_array_string, path_string):
        
        # Hardcoded values for internal logic
        NEWLINE_DELIMITER = '\n'
        NOT_FOUND_PLACEHOLDER = "" # Hardcoded to an empty string

        # 1. Parse the JSON Array String
        try:
            data_list = json.loads(json_array_string)
        except json.JSONDecodeError as e:
            error_msg = f"JSON Decode Error: Input is not a valid JSON string. {e}"
            print(f"Error: {error_msg}")
            return (error_msg,)
        
        if not isinstance(data_list, list):
            error_msg = "Input JSON must be an Array (list) of objects."
            print(f"Error: {error_msg}")
            return (error_msg,)
        
        # 2. Prepare the Path for Navigation
        path_parts = path_string.split('.')
        
        extracted_values = []
        
        # 3. Iterate, Extract, and Handle Errors
        for item in data_list:
            if isinstance(item, dict):
                # Use the hardcoded placeholder
                value = self._get_nested_value(item, path_parts, NOT_FOUND_PLACEHOLDER)
                extracted_values.append(value)
            else:
                # If an element in the array is not an object/dict, use placeholder
                extracted_values.append(NOT_FOUND_PLACEHOLDER)
        
        # 4. Concatenate the Extracted Values
        final_string = NEWLINE_DELIMITER.join(extracted_values)
        return (final_string,)