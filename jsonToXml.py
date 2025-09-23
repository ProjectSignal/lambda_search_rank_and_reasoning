from typing import Dict, Any
import xml.etree.ElementTree as ET
from xml.dom import minidom
import json

def sanitize_text(text: str) -> str:
    """Remove or replace invalid XML characters from text."""
    if not isinstance(text, str):
        text = str(text)
    # Remove invalid XML characters based on XML 1.0 spec
    def is_valid_xml_char(c):
        code = ord(c)
        return (
            code in (0x9, 0xA, 0xD) or
            (0x20 <= code <= 0xD7FF) or
            (0xE000 <= code <= 0xFFFD) or
            (0x10000 <= code <= 0x10FFFF)
        )
    text = ''.join(c for c in text if is_valid_xml_char(c))
    # Escape XML special characters
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace('\'', '&apos;')
    return text

def json_to_xml(node_data: Dict[str, Any]) -> str:
    """Convert node JSON data to XML format."""
    root = ET.Element("profile")

    # Basic profile information
    if node_data.get("name"):
        ET.SubElement(root, "name").text = sanitize_text(node_data["name"])
    if node_data.get("linkedinHeadline"):
         ET.SubElement(root, "linkedinHeadline").text = sanitize_text(node_data["linkedinHeadline"])
    if node_data.get("about"):
        ET.SubElement(root, "about").text = sanitize_text(node_data["about"])
    if node_data.get("currentLocation"):
        ET.SubElement(root, "currentLocation").text = sanitize_text(node_data["currentLocation"])
    # if node_data.get("backgroundImage"):
    #      ET.SubElement(root, "backgroundImage").text = sanitize_text(node_data["backgroundImage"]) # Assuming URL is safe

    # Comment out the contacts section
    # Contacts
    # if node_data.get("contacts"):
    #     contacts = ET.SubElement(root, "contacts")
    #     for key, value in node_data["contacts"].items():
    #         # Skip linkedin URL (and potentially others if added later)
    #         if key == "linkedin":
    #              continue
    #         if value:
    #             ET.SubElement(contacts, key).text = sanitize_text(value)

    # Education
    if node_data.get("education"):
        education = ET.SubElement(root, "education")
        for school_data in node_data["education"]:
            school = ET.SubElement(education, "school")
            # Map JSON keys to desired XML tags, handling potential missing keys
            field_mappings = {
                "school": "schoolName",
                "degree": "degree",
                "field_of_study": "fieldOfStudy",
                "dates": "duration",
                "description": "description",
                "activities": "activities",
                "grade": "grade"
            }
            for json_key, xml_tag in field_mappings.items():
                 if school_data.get(json_key):
                      ET.SubElement(school, xml_tag).text = sanitize_text(school_data[json_key])


    # Work Experience
    if node_data.get("workExperience"):
        work_experience = ET.SubElement(root, "workExperience")
        for job_data in node_data["workExperience"]:
            job = ET.SubElement(work_experience, "job")
             # Map JSON keys to desired XML tags, handling potential missing keys
            field_mappings = {
                "title": "title",
                "employmentType": "employmentType",
                "companyName": "companyName",
                # "companyUrl": "companyUrl", # Removed
                "companyIndustry": "companyIndustry",
                "location": "location",
                "duration": "duration",
                "description": "description",
                "about": "companyDescription",
                "specialties": "companySpecialties",
                
                # "companyLogo": "companyLogo", # Removed
                # "companyUsername": "companyUsername",
                # "companyStaffCountRange": "companyStaffCountRange"
            }
            for json_key, xml_tag in field_mappings.items():
                if job_data.get(json_key):
                    ET.SubElement(job, xml_tag).text = sanitize_text(job_data[json_key])

            # Company Info from webpage collection (assuming this is nested correctly)
            # if job_data.get("companyInfo"):
            #     company_info = ET.SubElement(job, "companyInfo")
            #     info_data = job_data["companyInfo"]
            #     # Add all company information fields dynamically
            #     for key, value in info_data.items():
            #          if value:
            #               # Convert camelCase/snake_case key to a simple tag if needed, or use as is
            #               # For simplicity, using the key directly after sanitization check
            #               ET.SubElement(company_info, key).text = sanitize_text(value)

    # Comment out the Skills section
    # if node_data.get("skills"):
    #     skills_elem = ET.SubElement(root, "skills")
    #     for skill in node_data["skills"]:
    #          if skill:
    #              ET.SubElement(skills_elem, "skill").text = sanitize_text(skill)

    # Accomplishments (Dynamically handle different types)
    if node_data.get("accomplishments"):
        accomplishments = ET.SubElement(root, "accomplishments")
        acc_data = node_data["accomplishments"]

        for acc_type, acc_list in acc_data.items():
            # Ensure the value is a list before iterating
            if isinstance(acc_list, list):
                acc_type_elem = ET.SubElement(accomplishments, acc_type) # e.g., <Certifications>, <Honors>
                for item_data in acc_list:
                     # Ensure the item in the list is a dictionary
                     if isinstance(item_data, dict):
                        item_elem = ET.SubElement(acc_type_elem, "item") # Generic item element
                        # Add all fields from the item's dictionary
                        for key, value in item_data.items():
                            # Skip logo fields
                            if key in ["certificateLogo", "issuerLogo"]:
                                continue
                            if value: # Only add if value is not None or empty
                                # Convert camelCase key to a simpler tag if needed, or use as is
                                # Example conversion (simple camelCase to lower):
                                # xml_tag = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
                                # For simplicity, using the key directly for now:
                                ET.SubElement(item_elem, key).text = sanitize_text(value)


    # Volunteering
    if node_data.get("volunteering"):
        volunteering_section = ET.SubElement(root, "volunteering")
        for vol_data in node_data["volunteering"]:
            volunteer_exp = ET.SubElement(volunteering_section, "experience")
            # Map JSON keys to desired XML tags, handling potential missing keys
            field_mappings = {
                "title": "title",
                "organizationName": "organizationName",
                # "organizationUrl": "organizationUrl", # Removed
                # "organizationLogo": "organizationLogo", # Already removed
                # "organizationId": "organizationId",
                "dateRange": "dateRange",
                "description": "description",
                "cause": "cause"
            }
            for json_key, xml_tag in field_mappings.items():
                if vol_data.get(json_key):
                    ET.SubElement(volunteer_exp, xml_tag).text = sanitize_text(vol_data[json_key])


    # Convert the tree to a string
    # Using xml.dom.minidom for pretty printing
    rough_string = ET.tostring(root, 'utf-8')
    from xml.dom import minidom
    reparsed = minidom.parseString(rough_string)
    # Get pretty XML and remove the declaration line
    xml_str = reparsed.toprettyxml(indent="  ")
    return '\n'.join(xml_str.split('\n')[1:])


if __name__ == "__main__":
    with open("nikita.json", "r", encoding="utf-8") as file:
        node_data = json.load(file)
    xml_str = json_to_xml(node_data)
    
    with open("nikita.xml", "w", encoding="utf-8") as file:
        file.write(xml_str)
