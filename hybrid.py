import os
import json
import time
import copy
import uuid
import certifi
from pymongo import MongoClient, InsertOne

# ----------------------------
# CONFIGURATION & UTILITIES
# ----------------------------
def load_config(config_file):
    try:
        with open(config_file, "r", encoding="utf-8") as file:
            config = json.load(file)
            print(f"[INFO] Loaded config from '{config_file}': {config}")
            return config
    except Exception as e:
        print(f"[ERROR] Loading config failed: {e}")
        exit(1)

# ----------------------------
# openEHR Processing Functions
# ----------------------------
ALLOWED_RM_TYPES = {
    "COMPOSITION", "SECTION", "ADMIN_ENTRY",
    "OBSERVATION", "EVALUATION", "INSTRUCTION",
    "ACTION", "CLUSTER", "ITEM_TREE", "ITEM_LIST",
    "ITEM_SINGLE", "ITEM_TABLE", "ELEMENT", "HISTORY",
    "EVENT", "POINT_EVENT", "INTERVAL_EVENT", "ACTIVITY", 
    "ISM_TRANSITION", "INSTRUCTION_DETAILS", "CARE_ENTRY", 
    "PARTY_PROXY", "EVENT_CONTEXT"
}

ABBREV_MAP = {
    "COMPOSITION": "CO",
    "SECTION": "SE",
    "ADMIN_ENTRY": "AE",
    "OBSERVATION": "OB",
    "EVALUATION": "EV",
    "INSTRUCTION": "IN",
    "ACTION": "AC",
    "CLUSTER": "CL",
    "ITEM_TREE": "IT",
    "ITEM_LIST": "IL",
    "ITEM_SINGLE": "IS",
    "ITEM_TABLE": "TB",
    "ELEMENT": "EL",
    "HISTORY": "HI",
    "EVENT": "ET",
    "POINT_EVENT": "PE",
    "INTERVAL_EVENT": "IE",
    "ACTIVITY": "AY",
    "ISM_TRANSITION": "ST",
    "INSTRUCTION_DETAILS": "ID",
    "CARE_ENTRY": "CE",
    "PARTY_PROXY": "PP",
    "EVENT_CONTEXT": "EC",
    "openEHR-EHR-COMPOSITION": "CO",
    "openEHR-EHR-SECTION": "SE",
    "openEHR-EHR-ADMIN_ENTRY": "AE",
    "openEHR-EHR-OBSERVATION": "OB",
    "openEHR-EHR-EVALUATION": "EV",
    "openEHR-EHR-INSTRUCTION": "IN",
    "openEHR-EHR-ACTION": "AC",
    "openEHR-EHR-CLUSTER": "CL",
    "openEHR-EHR-ITEM_TREE": "IT",
    "openEHR-EHR-ITEM_LIST": "IL",
    "openEHR-EHR-ITEM_SINGLE": "IS",
    "openEHR-EHR-ITEM_TABLE": "TB",
    "openEHR-EHR-ELEMENT": "EL",
    "openEHR-EHR-HISTORY": "HI",
    "openEHR-EHR-EVENT": "ET",
    "openEHR-EHR-POINT_EVENT": "PE",
    "openEHR-EHR-INTERVAL_EVENT": "IE",
    "openEHR-EHR-ACTIVITY": "AY",
    "openEHR-EHR-ISM_TRANSITION": "ST",
    "openEHR-EHR-INSTRUCTION_DETAILS": "ID",
    "openEHR-EHR-CARE_ENTRY": "CE",
    "openEHR-EHR-PARTY_PROXY": "PP",
    "openEHR-EHR-EVENT_CONTEXT": "EC"
}

def abbreviate_text(text):
    if not isinstance(text, str):
        return text
    for long_str in sorted(ABBREV_MAP, key=len, reverse=True):
        abbr = ABBREV_MAP[long_str]
        if long_str in text:
            text = text.replace(long_str, abbr)
    return text

def get_archetype_id(node_dict):
    if not isinstance(node_dict, dict):
        return None
    if "archetype_details" in node_dict and isinstance(node_dict["archetype_details"], dict):
        maybe_id = node_dict["archetype_details"].get("archetype_id", {})
        return maybe_id.get("value")
    return node_dict.get("archetype_node_id")

def is_openehr_node(node_dict):
    if not isinstance(node_dict, dict):
        return False
    rm_type = node_dict.get("_type", "")
    if rm_type in ALLOWED_RM_TYPES:
        return True
    if "archetype_details" in node_dict or "archetype_node_id" in node_dict:
        return True
    return False

def contains_openehr_node(obj):
    if is_openehr_node(obj):
        return True
    if isinstance(obj, dict):
        for v in obj.values():
            if contains_openehr_node(v):
                return True
        return False
    if isinstance(obj, list):
        for item in obj:
            if contains_openehr_node(item):
                return True
        return False
    return False

def flatten_node(node, path="canonicalJSON", archetype_path="canonicalJSON", ancestors=None):
    if ancestors is None:
        ancestors = []
    flattened = []
    recognized = is_openehr_node(node)
    current_ancestors = ancestors

    if recognized:
        node_data = {}
        if isinstance(node, dict):
            for key, val in node.items():
                if contains_openehr_node(val):
                    continue
                node_data[key] = val
        if ABBREVIATE_ENABLED:
            abbr_archetype_path = abbreviate_text(archetype_path)
            abbr_ancestors = [abbreviate_text(a) for a in current_ancestors]
        else:
            abbr_archetype_path = archetype_path
            abbr_ancestors = current_ancestors
        flattened.append({
            "path": abbr_archetype_path,
            "ant": abbr_ancestors,
            "data": node_data
        })
        this_id = get_archetype_id(node)
        if this_id:
            child_ancestors = ancestors + [this_id]
        else:
            child_ancestors = ancestors
    else:
        child_ancestors = ancestors

    if isinstance(node, dict):
        for key, val in node.items():
            new_path = f"{path}.{key}"
            if isinstance(val, dict):
                if is_openehr_node(val) or contains_openehr_node(val):
                    child_archetype_id = get_archetype_id(val)
                    if child_archetype_id:
                        child_archetype_path = f"{archetype_path}.{key}[{child_archetype_id}]"
                    else:
                        child_archetype_path = f"{archetype_path}.{key}"
                    if ABBREVIATE_ENABLED:
                        child_archetype_path = abbreviate_text(child_archetype_path)
                    flattened += flatten_node(val, new_path, child_archetype_path, child_ancestors)
            elif isinstance(val, list):
                for idx, item in enumerate(val):
                    item_path = f"{new_path}[{idx}]"
                    if is_openehr_node(item) or contains_openehr_node(item):
                        child_archetype_id = get_archetype_id(item)
                        if child_archetype_id:
                            item_archetype_path = f"{archetype_path}.{key}[{child_archetype_id}]"
                        else:
                            item_archetype_path = f"{archetype_path}.{key}[{idx}]"
                        if ABBREVIATE_ENABLED:
                            item_archetype_path = abbreviate_text(item_archetype_path)
                        flattened += flatten_node(item, item_path, item_archetype_path, child_ancestors)
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            item_path = f"{path}[{idx}]"
            if is_openehr_node(item) or contains_openehr_node(item):
                child_archetype_id = get_archetype_id(item)
                if child_archetype_id:
                    item_archetype_path = f"{archetype_path}[{child_archetype_id}]"
                else:
                    item_archetype_path = f"{archetype_path}[{idx}]"
                if ABBREVIATE_ENABLED:
                    item_archetype_path = abbreviate_text(item_archetype_path)
                flattened += flatten_node(item, item_path, item_archetype_path, child_ancestors)
    return flattened

def process_document(doc):
    """
    Process a source document:
      - Flatten canonicalJSON into nodes.
      - Preserve the original document (including its original ehr_id, if available).
    """
    canonical = doc.get("canonicalJSON", {})
    nodes = flatten_node(canonical, "canonicalJSON", "canonicalJSON", [])
    return {
        "ehr_id": doc.get("ehr_id", None),
        "nodes": nodes,
        "original": doc
    }

def extract_searchable_nodes(nodes, search_config):
    """
    Extract only the nodes that should be included in search_nodes based on configuration.
    """
    if not search_config:
        return []
    
    searchable_nodes = []
    search_paths = search_config.get("search_paths", [])
    
    for node in nodes:
        # Check if this node's path matches any of the specified search paths
        for search_path in search_paths:
            if (search_path in node["path"] or 
                any(search_path in ancestor for ancestor in node["ant"])):
                searchable_nodes.append(node)
                break
    
    return searchable_nodes

# ----------------------------
# Synthetic Grouping Functions
# ----------------------------
def generate_synthetic_groups_by_ehr(processed_docs, min_comps, num_patients):
    groups_by_ehr = {}
    for doc in processed_docs:
        orig_ehr = doc["original"].get("ehr_id")
        if not orig_ehr:
            orig_ehr = str(uuid.uuid4())
        groups_by_ehr.setdefault(orig_ehr, []).append(doc)
    
    # Filter out groups with fewer than min_comps compositions
    unique_groups = [group for group in groups_by_ehr.values() if len(group) >= min_comps]
    print(f"[INFO] Found {len(groups_by_ehr)} patient groups; {len(unique_groups)} remain after filtering by min_compositions={min_comps}.")
    
    # Replicate groups to reach the target synthetic patient count
    synthetic_groups = copy.deepcopy(unique_groups)
    while len(synthetic_groups) < num_patients:
        for grp in unique_groups:
            if len(synthetic_groups) >= num_patients:
                break
            synthetic_groups.append(copy.deepcopy(grp))
    synthetic_groups = synthetic_groups[:num_patients]
    
    # Assign new synthetic ehr_ids
    for idx, group in enumerate(synthetic_groups):
        new_ehr = str(uuid.uuid4())
        for doc in group:
            doc["ehr_id"] = new_ehr
        print(f"[INFO] Assigned synthetic ehr_id {new_ehr} to group {idx+1}/{len(synthetic_groups)}")
    
    return synthetic_groups

# Assign a synthetic composition ID to each composition in a group
def assign_synthetic_composition_ids(synthetic_groups):
    for group in synthetic_groups:
        for doc in group:
            doc["synth_comp_id"] = str(uuid.uuid4())

# ----------------------------
# MAIN PROCESSING FUNCTION
# ----------------------------
def process_all_documents(connection_string, config):
    global ABBREVIATE_ENABLED
    ABBREVIATE_ENABLED = config.get("abbreviate", False)
    print(f"[INFO] Abbreviation enabled: {ABBREVIATE_ENABLED}")
    source_db_name = config["source_database_name"]
    source_col_name = config["source_collection"]
    target_col_name = config["target_collection"]
    batch_size = config.get("batch_size", 1000)
    doc_limit = config.get("limit", None)
    synth_params = config.get("synthetic_generation", {})
    min_comps = synth_params.get("min_compositions", 3)
    num_patients = synth_params.get("num_patients", 30000)
    clean = config.get("clean_collections", True)
    search_config = config.get("search_config", {})

    print("[INFO] Connecting to MongoDB...")
    client = MongoClient(connection_string, tlsCAFile=certifi.where())
    db = client[source_db_name]
    src = db[source_col_name]
    
    # Define target collection
    target_collection = db[target_col_name]

    if clean:
        print("[INFO] Clearing target collection...")
        target_collection.delete_many({})
    else:
        print("[INFO] Keeping existing data; new documents will be added.")

    processed_docs = []
    count = 0
    t0 = time.time()

    # Process source documents
    for doc in src.find({}):
        processed_docs.append(process_document(doc))
        count += 1
        if count % 1000 == 0:
            print(f"[INFO] Processed {count} documents...")
        if doc_limit and count >= doc_limit:
            break
    print(f"[INFO] Finished processing {count} documents in {time.time()-t0:.2f} seconds.")

    # ----------------------------
    # Synthetic Grouping
    # ----------------------------
    synthetic_groups = generate_synthetic_groups_by_ehr(processed_docs, min_comps, num_patients)
    
    # Assign synthetic composition ids
    assign_synthetic_composition_ids(synthetic_groups)

    # ----------------------------
    # Build and Insert Documents
    # ----------------------------
    bulk_docs = []
    total_inserted = 0

    for group_idx, group in enumerate(synthetic_groups):
        for doc in group:
            # Create the hybrid document with search_nodes and comp_nodes
            hybrid_doc = {
                "_id": doc["synth_comp_id"],
                "comp_id": doc["synth_comp_id"],
                "ehr_id": doc["ehr_id"],
                "comp_nodes": doc["nodes"],
                "search_nodes": extract_searchable_nodes(doc["nodes"], search_config)
            }
            
            # Add composition date if available
            for node in doc["nodes"]:
                if "composition_date" in node["data"]:
                    hybrid_doc["composition_date"] = node["data"]["composition_date"]
                    break
                
                # Also look for context/start_time which is often the composition date
                if node["path"].endswith("context") and "start_time" in node["data"]:
                    hybrid_doc["composition_date"] = node["data"]["start_time"]["value"]
                    break
            
            bulk_docs.append(InsertOne(hybrid_doc))
            total_inserted += 1
            
            # Insert in batches
            if len(bulk_docs) >= batch_size:
                target_collection.bulk_write(bulk_docs)
                print(f"[INFO] Inserted batch of {len(bulk_docs)} documents. Total processed: {total_inserted}")
                bulk_docs = []
    
    # Insert any remaining documents
    if bulk_docs:
        target_collection.bulk_write(bulk_docs)
        print(f"[INFO] Inserted final batch of {len(bulk_docs)} documents. Total processed: {total_inserted}")

    upload_time = time.time() - t0
    print(f"[INFO] All documents have been inserted. Total upload time: {upload_time:.2f} seconds.")
    client.close()
    print("[INFO] MongoDB connection closed.")

# ----------------------------
# MAIN ENTRY POINT
# ----------------------------
if __name__ == "__main__":
    config_file = "config.json"
    cfg = load_config(config_file)
    for key in ["source_database_name", "source_collection", "target_collection"]:
        if key not in cfg:
            print(f"[ERROR] Missing config key: '{key}'")
            exit(1)
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        print("[ERROR] MONGO_URI not set in environment")
        exit(1)
    print("[INFO] Starting synthetic processing with hybrid-nodes strategy...")
    process_all_documents(mongo_uri, cfg)
    print("[INFO] Done.")
