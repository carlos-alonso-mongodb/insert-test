#!/usr/bin/env python3
import os
import json
import time
import uuid
import logging
import threading
import certifi
import gc
import math
import re   # <-- MAKE SURE THIS IS HERE IF YOU USE `re.match(...)`

from pymongo import MongoClient
from dateutil.parser import parse as parse_date
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logging setup (ensure the parenthesis is closed):
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(threadName)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# --------------------------------------------------
# Global / Stats
# --------------------------------------------------
stats_lock = threading.Lock()
global_stats = {
    "patients_processed": 0,
    "compositions_read": 0,
    "synthetic_docs_inserted": 0
}
thread_stats = {}
PROCESSING_DONE = False

CONFIG_SHORTCUTS_ENABLED = False
CONFIG_SHORTCUTS = {}
SEARCH_NODES_CONFIG = {}

# ----------------------------
# Utility / Shortcut Functions
# ----------------------------
def generate_unique_id():
    return str(uuid.uuid4())

def load_config(config_file):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
            logging.info(f"Loaded config from '{config_file}'")
            return config
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        exit(1)

def abbreviate_text(text):
    if not isinstance(text, str):
        return text
    for long_str in sorted(CONFIG_SHORTCUTS, key=len, reverse=True):
        abbr = CONFIG_SHORTCUTS[long_str]
        if long_str in text:
            text = text.replace(long_str, abbr)
    return text

def apply_field_shortcuts(obj):
    """Apply shortcuts to dict keys/values recursively."""
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            new_key = CONFIG_SHORTCUTS.get(k, k)
            new_dict[new_key] = apply_field_shortcuts(v)
        return new_dict
    elif isinstance(obj, list):
        return [apply_field_shortcuts(item) for item in obj]
    elif isinstance(obj, str):
        return abbreviate_text(obj)
    else:
        return obj

def convert_field_value(field):
    """
    Convert recognized DV_* structures to python types.
    E.g. DV_DATE_TIME => Python datetime, DV_QUANTITY => float, etc.
    """
    if isinstance(field, dict):
        clone = dict(field)
        # Check typical pattern: "T" or "_type" plus "v" or "value"
        if "T" in clone and "v" in clone:
            type_code = clone["T"]
            val_key = "v"
        elif "_type" in clone and "value" in clone:
            type_code = clone["_type"]
            val_key = "value"
        else:
            # If we don't see a recognized pattern, recurse deeper
            for k, v in clone.items():
                clone[k] = convert_field_value(v)
            return clone

        try:
            if type_code in ["ddt", "DV_DATE_TIME", "dd", "DV_DATE", "dti", "DV_TIME"]:
                if val_key in clone:
                    raw_str = clone[val_key]
                    clone[val_key] = parse_date(raw_str)
                return clone
            elif type_code in ["dq", "dc", "DV_QUANTITY", "DV_COUNT"]:
                if "magnitude" in clone:
                    mag = clone["magnitude"]
                    clone["magnitude"] = float(mag) if isinstance(mag, str) and '.' in mag else int(mag)
                elif val_key in clone:
                    val = clone[val_key]
                    clone[val_key] = float(val) if isinstance(val, str) and '.' in val else int(val)
                return clone
            elif type_code in ["DV_ORDINAL", "DO"]:
                if val_key in clone:
                    clone[val_key] = int(clone[val_key])
                return clone
            return clone
        except Exception as e:
            logging.debug(f"Conversion error in field {field}: {e}")
            return clone

    elif isinstance(field, list):
        return [convert_field_value(item) for item in field]
    else:
        return field

# ----------------------------
# openEHR Node Utility Functions
# ----------------------------
ALLOWED_RM_TYPES = {
    "COMPOSITION", "SECTION", "ADMIN_ENTRY", "OBSERVATION", "EVALUATION",
    "INSTRUCTION", "ACTION", "CLUSTER", "ITEM_TREE", "ITEM_LIST",
    "ITEM_SINGLE", "ITEM_TABLE", "ELEMENT", "HISTORY", "EVENT", "POINT_EVENT",
    "INTERVAL_EVENT", "ACTIVITY", "ISM_TRANSITION", "INSTRUCTION_DETAILS",
    "CARE_ENTRY", "PARTY_PROXY", "EVENT_CONTEXT"
}

def is_openehr_node(node):
    if not isinstance(node, dict):
        return False
    rm_type = node.get("_type", "")
    if rm_type in ALLOWED_RM_TYPES:
        return True
    if "archetype_details" in node or "archetype_node_id" in node:
        return True
    return False

def contains_openehr_node(obj):
    if is_openehr_node(obj):
        return True
    if isinstance(obj, dict):
        return any(contains_openehr_node(v) for v in obj.values())
    if isinstance(obj, list):
        return any(contains_openehr_node(item) for item in obj)
    return False

def get_archetype_id(node):
    if not isinstance(node, dict):
        return None
    if "archetype_details" in node and isinstance(node["archetype_details"], dict):
        aid = node["archetype_details"].get("archetype_id", {})
        return aid.get("value") if isinstance(aid, dict) else aid
    return node.get("archetype_node_id")

def flatten_node(node, path="comp", archetype_path="comp", ancestors=None):
    """
    Recursively traverse the composition tree to produce a list of flattened nodes.
    Each flattened node is stored as a dict with:
      - "p": the current path in the tree,
      - "d": the node data (immediate non-nested fields),
      - "a": optional array of ancestor archetype IDs.
    """
    if ancestors is None:
        ancestors = []
    flattened = []
    recognized = is_openehr_node(node)

    if recognized:
        node_data = {}
        if isinstance(node, dict):
            for key, val in node.items():
                if not contains_openehr_node(val):
                    node_data[key] = val
        node_data = convert_field_value(node_data)
        flat_entry = {"p": archetype_path, "d": node_data}
        if ancestors:
            flat_entry["a"] = ancestors
        flattened.append(flat_entry)
        this_id = get_archetype_id(node)
        child_ancestors = ancestors + [this_id] if this_id else ancestors
    else:
        child_ancestors = ancestors

    if isinstance(node, dict):
        for key, val in node.items():
            new_path = f"{path}.{key}"
            if isinstance(val, dict):
                if is_openehr_node(val) or contains_openehr_node(val):
                    cid = get_archetype_id(val)
                    child_path = f"{archetype_path}.{key}[{cid}]" if cid else f"{archetype_path}.{key}"
                    flattened.extend(flatten_node(val, new_path, child_path, child_ancestors))
            elif isinstance(val, list):
                for idx, item in enumerate(val):
                    if is_openehr_node(item) or contains_openehr_node(item):
                        item_path = f"{new_path}[{idx}]"
                        cid = get_archetype_id(item)
                        child_path = f"{archetype_path}.{key}[{cid}]" if cid else f"{archetype_path}.{key}[{idx}]"
                        flattened.extend(flatten_node(item, item_path, child_path, child_ancestors))
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            item_path = f"{path}[{idx}]"
            cid = get_archetype_id(item)
            child_path = f"{archetype_path}[{cid}]" if cid else f"{archetype_path}[{idx}]"
            flattened.extend(flatten_node(item, item_path, child_path, child_ancestors))

    return flattened

def extract_archetype_id_from_comp(comp):
    """
    Traverse a composition to extract the top-level composition archetype ID.
    """
    if not comp:
        return None
    if isinstance(comp, dict):
        if "archetype_details" in comp:
            ad = comp["archetype_details"]
            if isinstance(ad, dict) and "archetype_id" in ad:
                aid = ad["archetype_id"]
                return aid.get("value") if isinstance(aid, dict) else aid
        for v in comp.values():
            aid = extract_archetype_id_from_comp(v)
            if aid:
                return aid
    elif isinstance(comp, list):
        for item in comp:
            aid = extract_archetype_id_from_comp(item)
            if aid:
                return aid
    return None

# ----------------------------
# Build Search Nodes (AQL-style)
# ----------------------------
def build_search_nodes(comp_nodes, search_mapping):
    """
    Constructs 'sn' (search nodes) by:
     1) finding flattened nodes with the specified archetype_node_id
     2) for each data path, extracting sub-fields
     3) returning them under 'd'
    """
    version = search_mapping.get("version", 1)
    mappings = search_mapping.get("mappings", [])
    result_nodes = []

    for mapping in mappings:
        desired_ai = mapping.get("archetype_node_id")
        create_ancestors = str(mapping.get("createAncestorsArray", "false")).lower() == "true"
        data_paths = mapping.get("data", [])

        for node in comp_nodes:
            node_ai = node.get("d", {}).get("archetype_node_id") or node.get("d", {}).get("ani")
            if node_ai == desired_ai:
                out_node = {}
                if create_ancestors and "a" in node:
                    out_node["a"] = node["a"]

                d_out = {}
                for aql_path in data_paths:
                    value = get_value_by_aql_path(comp_nodes, node, aql_path)
                    if value is not None:
                        final_key = aql_path.strip("/").split("/")[-1]
                        d_out[final_key] = value
                out_node["d"] = d_out
                result_nodes.append(out_node)

    return {"version": version, "nodes": result_nodes}

def get_value_by_aql_path(all_nodes, start_node, aql_path):
    segments = [seg for seg in aql_path.split('/') if seg]
    current_nodes = [start_node]

    for seg in segments:
        m = re.match(r'^(.*?)\[(.+)\]$', seg)  # e.g. items[at0007]
        if m:
            child_ai = m.group(2)
            new_nodes = []
            for node in current_nodes:
                new_nodes.extend(find_child_nodes(all_nodes, node, child_ai))
            current_nodes = new_nodes
        else:
            subkeys = seg.split('/')
            new_nodes = []
            for node in current_nodes:
                d = node.get("d", {})
                val = get_dict_path(d, subkeys)
                if val is None:
                    continue
                if isinstance(val, list):
                    for itm in val:
                        if isinstance(itm, dict):
                            new_nodes.append({"d": itm, "a": node.get("a", [])})
                        else:
                            new_nodes.append({"d": {"__primitive": itm}, "a": node.get("a", [])})
                elif isinstance(val, dict):
                    new_nodes.append({"d": val, "a": node.get("a", [])})
                else:
                    new_nodes.append({"d": {"__primitive": val}, "a": node.get("a", [])})
            current_nodes = new_nodes

    results = []
    for n in current_nodes:
        if "__primitive" in n["d"]:
            results.append(n["d"]["__primitive"])
        else:
            if n["d"]:
                results.append(n["d"])

    if not results:
        return None
    return results[0] if len(results) == 1 else results

def find_child_nodes(all_nodes, parent_node, child_ai):
    parent_ai = parent_node.get("d", {}).get("archetype_node_id") or parent_node.get("d", {}).get("ani")
    parent_ancestors = parent_node.get("a", [])
    required_a = parent_ancestors + [parent_ai]

    matches = []
    for candidate in all_nodes:
        cand_ai = candidate.get("d", {}).get("archetype_node_id") or candidate.get("d", {}).get("ani")
        cand_a = candidate.get("a", [])
        if cand_ai == child_ai and cand_a == required_a:
            matches.append(candidate)
    return matches

def get_dict_path(obj, path_segments):
    current = obj
    for seg in path_segments:
        if not isinstance(current, dict) or seg not in current:
            return None
        current = current[seg]
    return current


# ----------------------------
# Replication + Transform
# ----------------------------
# --------------------------------------------------
# Step 1: Transform a single composition (flatten + sn + shortcuts)
#         Return the final “template doc” WITHOUT setting ._id or .ehr_id
# --------------------------------------------------
def transform_composition_to_template(source_doc):
    """
    Flatten comp => produce { 'version': x, 'cn': [...], 'sn': [...], 'snv': 1? }
    but do NOT set _id or ehr_id. We'll set those during replication.
    """
    comp_obj = source_doc.get("comp", {})
    flattened = flatten_node(comp_obj, "comp", "comp", [])
    top_archetype_id = extract_archetype_id_from_comp(comp_obj)

    out_doc = {
        "version": source_doc.get("version"),
        "cn": flattened
    }
    if top_archetype_id and top_archetype_id in SEARCH_NODES_CONFIG:
        mapping = SEARCH_NODES_CONFIG[top_archetype_id]
        sn_result = build_search_nodes(flattened, mapping)
        out_doc["sn"] = sn_result["nodes"]
        out_doc["snv"] = sn_result.get("version")

    if CONFIG_SHORTCUTS_ENABLED:
        out_doc = apply_field_shortcuts(out_doc)

    return out_doc

# --------------------------------------------------
# Step 2: Replicate entire set of compositions for a single EHR
# --------------------------------------------------
def replicate_entire_ehr(ehr_id, transformed_comps, replication_factor, batch_size, target_coll):
    """
    We have 'transformed_comps': a list of N template docs (one for each composition).
    We want to replicate them replication_factor times => total N * replication_factor documents.

    For each copy i, we create one new EHR ID (shared by all the compositions).
    Then each composition gets a new _id.

    We'll do batch inserts of size 'batch_size' to reduce memory usage.
    """
    total_docs = len(transformed_comps) * replication_factor
    synthetic_buffer = []
    inserted_count = 0

    # Pre-generate all the new EHR IDs for this patient
    new_ehr_ids = [generate_unique_id() for _ in range(replication_factor)]

    for i, new_ehr_id in enumerate(new_ehr_ids):
        for template_doc in transformed_comps:
            new_doc = {
                "_id": generate_unique_id(),
                "ehr_id": new_ehr_id,
                # copy all other fields from template
                "version": template_doc["version"],
                "cn": template_doc["cn"]
            }
            if "sn" in template_doc:
                new_doc["sn"] = template_doc["sn"]
            if "snv" in template_doc:
                new_doc["snv"] = template_doc["snv"]

            synthetic_buffer.append(new_doc)

        # If buffer is large enough or this is the last iteration -> do an insert
        if len(synthetic_buffer) >= batch_size or (i == replication_factor - 1):
            target_coll.insert_many(synthetic_buffer, ordered=False)
            inserted_count += len(synthetic_buffer)
            synthetic_buffer = []

    return total_docs  # same as inserted_count

# --------------------------------------------------
# Worker: Process a single patient (EHR) fully
# --------------------------------------------------
def process_one_ehr(ehr_id, source_coll, target_coll, replication_factor, batch_size):
    """
    1) Lock and read all docs for this ehr_id
    2) Transform each doc into a 'template doc'
    3) Replicate the entire set of templates
       => replication_factor new EHR IDs
       => each composition gets a new _id
    4) Insert into target in batches
    """
    thread_name = threading.current_thread().name
    start_t = time.time()

    # Lock all docs for that EHR in one shot
    update_res = source_coll.update_many(
        {"ehr_id": ehr_id, "used": False},
        {"$set": {"used": True}}
    )
    locked_docs = list(source_coll.find({"ehr_id": ehr_id, "used": True}))

    comps_count = len(locked_docs)
    if comps_count == 0:
        logging.info(f"[{thread_name}] EHR={ehr_id}: no docs to process or they were already used.")
        return (0, 0)

    # Transform each composition to a "template doc"
    transformed_comps = []
    for doc in locked_docs:
        # Flatten, build sn, apply shortcuts, etc
        transformed = transform_composition_to_template(doc)
        transformed_comps.append(transformed)

    # Now replicate them all for replication_factor
    # so that all compositions in this EHR share the *same* new EHR ID for each replicate
    total_synth_docs = replicate_entire_ehr(
        ehr_id,
        transformed_comps,
        replication_factor,
        batch_size,
        target_coll
    )

    end_t = time.time()
    logging.info(f"[{thread_name}] EHR={ehr_id}, comps={comps_count} => synthetic={total_synth_docs} inserted.")
    with stats_lock:
        thread_stats.setdefault(thread_name, {"tasks": 0, "busy_time": 0.0})
        thread_stats[thread_name]["tasks"] += 1
        thread_stats[thread_name]["busy_time"] += (end_t - start_t)

    return (comps_count, total_synth_docs)

# ----------------------------
# Periodic Logger
# ----------------------------
def periodic_logger(total_patients, interval=120, start_time=None):
    """
    Periodically logs:
      - Progress bar of patients processed vs total
      - # patients, comps, docs processed since last interval
      - Averages per patient (comps, docs)
      - Thread usage info
    """
    if start_time is None:
        start_time = time.time()

    last_p_done = 0
    last_comps_read = 0
    last_docs_inserted = 0

    # How wide the progress bar is (in characters)
    bar_length = 30

    while True:
        time.sleep(interval)
        now = time.time()
        elapsed = now - start_time

        with stats_lock:
            p_done = global_stats["patients_processed"]
            comps_read = global_stats["compositions_read"]
            docs_inserted = global_stats["synthetic_docs_inserted"]

        # Calculate progress/deltas
        p_delta = p_done - last_p_done
        c_delta = comps_read - last_comps_read
        d_delta = docs_inserted - last_docs_inserted

        last_p_done = p_done
        last_comps_read = comps_read
        last_docs_inserted = docs_inserted

        pct_done = (p_done / total_patients * 100) if total_patients else 0

        # Make an ASCII progress bar
        filled = int(bar_length * p_done / total_patients) if total_patients else 0
        bar = "#" * filled + "-" * (bar_length - filled)

        # Averages per patient (if p_done > 0)
        if p_done > 0:
            avg_comps = comps_read / p_done
            avg_docs = docs_inserted / p_done
        else:
            avg_comps = 0
            avg_docs = 0

        # Log summary
        logging.info(
            f"[Periodic] Processed {p_done}/{total_patients} EHRs "
            f"({pct_done:.1f}%)  |{bar}| \n"
            f"  Elapsed: {elapsed:.1f}s  |  "
            f"Comps read (total): {comps_read},  Synthetic inserted (total): {docs_inserted} \n"
            f"  Since last {interval}s => +{p_delta} patients, +{c_delta} comps, +{d_delta} docs \n"
            f"  Averages => comps/patient: {avg_comps:.2f}, docs/patient: {avg_docs:.2f}"
        )

        # Thread usage info
        with stats_lock:
            logging.info("[Thread Usage Report]:")
            for tname, usage in thread_stats.items():
                logging.info(
                    f"  {tname}: tasks={usage['tasks']}, "
                    f"busy_time={usage['busy_time']:.1f}s"
                )

        # Exit if done
        if PROCESSING_DONE or (p_done >= total_patients):
            break

# --------------------------------------------------
# Main
# --------------------------------------------------
def main(config_file="config.json"):
    global PROCESSING_DONE, CONFIG_SHORTCUTS_ENABLED, CONFIG_SHORTCUTS, SEARCH_NODES_CONFIG

    start_time = time.time()

    # 1) Load config
    cfg = load_config(config_file)

    source_conn_str = cfg["source"]["connection_string"]
    source_db_name = cfg["source"]["database_name"]
    source_coll_name = cfg["source"]["collection_name"]

    target_conn_str = cfg["target"]["connection_string"]
    target_db_name = cfg["target"]["database_name"]
    target_coll_name = cfg["target"]["collection_name"]

    max_workers = cfg.get("max_workers", 4)
    replication_factor = cfg.get("replication_factor", 180)
    patient_limit = cfg.get("patient_limit", 100)
    reset_used = cfg.get("reset_used", True)
    clean_collections = cfg.get("clean_collections", False)
    batch_size = cfg.get("batch_size", 2500)

    CONFIG_SHORTCUTS_ENABLED = cfg.get("shortcuts_enabled", False)
    CONFIG_SHORTCUTS = cfg.get("shortcuts", {})
    SEARCH_NODES_CONFIG = cfg.get("search_nodes", {})

    # 2) Connect to Mongo
    ca_path = certifi.where()
    source_client = MongoClient(source_conn_str, tlsCAFile=ca_path)
    source_db = source_client[source_db_name]
    source_coll = source_db[source_coll_name]

    target_client = MongoClient(target_conn_str, tlsCAFile=ca_path)
    target_db = target_client[target_db_name]
    target_coll = target_db[target_coll_name]

    # 3) Clean target if needed
    if clean_collections:
        logging.info(f"Cleaning target collection {target_db_name}.{target_coll_name} ...")
        target_coll.delete_many({})

    # 4) Reset 'used' flag if needed
    if reset_used:
        logging.info("Resetting 'used' flag on source docs.")
        source_coll.update_many({"used": True}, {"$set": {"used": False}})

    # 5) Find distinct EHR IDs
    pipeline = [
        {"$match": {"used": False}},
        {"$group": {"_id": "$ehr_id"}},
        {"$limit": patient_limit}
    ]
    distinct_ehr_ids = list(source_coll.aggregate(pipeline))
    total_patients = len(distinct_ehr_ids)
    logging.info(f"Found {total_patients} EHRs to process with replication_factor={replication_factor}")

    if total_patients == 0:
        logging.info("No patients found. Exiting.")
        return

    # 6) Start the periodic logger with the correct total_patients
    logger_thread = threading.Thread(
        target=periodic_logger,
        args=(total_patients, 60, start_time),  # pass start_time if you want
        daemon=True
    )
    logger_thread.start()

    # 7) Process EHRs in parallel
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for d in distinct_ehr_ids:
            ehr_id = d["_id"]
            fut = executor.submit(
                process_one_ehr,
                ehr_id,
                source_coll,
                target_coll,
                replication_factor,
                batch_size
            )
            futures.append(fut)

        for fut in as_completed(futures):
            try:
                comps_count, synth_count = fut.result()
            except Exception as ex:
                logging.error(f"Error: {ex}", exc_info=True)
                continue

            # 7a) Update global stats
            with stats_lock:
                global_stats["compositions_read"] += comps_count
                global_stats["synthetic_docs_inserted"] += synth_count
                global_stats["patients_processed"] += 1

                p_done = global_stats["patients_processed"]
                logging.info(f"[Patient Done] => {p_done}/{total_patients} done. comps={comps_count}, synth={synth_count}.")
                gc.collect()

    # 8) We're finished
    PROCESSING_DONE = True
    logger_thread.join()

    # 9) Print final summary
    with stats_lock:
        final_patients = global_stats["patients_processed"]
        final_comps = global_stats["compositions_read"]
        final_synth = global_stats["synthetic_docs_inserted"]

    elapsed = time.time() - start_time
    logging.info("Processing complete.")
    logging.info(f"EHRs processed: {final_patients}/{total_patients}")
    logging.info(f"Compositions read: {final_comps}")
    logging.info(f"Synthetic docs inserted: {final_synth}")
    logging.info(f"Elapsed time: {elapsed:.1f}s")

    # 10) Close clients
    source_client.close()
    target_client.close()


if __name__ == "__main__":
    main("config.json")