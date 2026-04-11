#!/usr/bin/env python3
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
"""
Seed Script - populates DynamoDB with categories, retailers, and global
keyword mappings (store_id = "global") as the baseline for the hybrid
store-scoped matching system.

Store-scoped mappings (store_id = "tesco", "aldi", etc.) are built
automatically as receipts are scanned and learned from.

Usage:
    python seed_data.py --env dev
    python seed_data.py --env prod
    python seed_data.py --env dev --profile my-aws-profile
    python seed_data.py --env dev --mappings-table grocery-scanner-dev-MappingsTable-ABC123
      (use --mappings-table when CloudFormation auto-generated the mappings table name)
"""

import argparse
import boto3
from datetime import datetime, timezone

GLOBAL_STORE = "global"

# =============================================================================
# CATEGORIES
# =============================================================================
CATEGORIES = [
    {
        "category_id": "alcohol",
        "name": "Alcohol",
        "icon": "🍷",
        "keywords": [
            "armagnac", "beers", "cider", "cocktail", "cognac",
            "cointreau", "gin", "grappa", "limoncello", "liqueur", "prosecco",
            "red wine", "rose wine", "vodka", "whiskey", "white wine"
        ]
    },
    {
        "category_id": "baby-stuff",
        "name": "Baby Stuff",
        "icon": "🍼",
        "keywords": [
            "aptamil", "baby bottles", "baby food", "baby latch", "baby powder",
            "kids pencils","pampers", 
            "pregnancy test", "shoes", "sticky things", "straws", 
            "toys"
        ]
    },
    {
        "category_id": "baking",
        "name": "Baking",
        "icon": "🎂",
        "keywords": [
            "cacao", "cake equipment", "food colour", "sweets", "vanilla",
            "yeast"
        ]
    },
    {
        "category_id": "barbecue",
        "name": "Barbecue",
        "icon": "🔥",
        "keywords": [
            "bbq tray"
        ]
    },
    {
        "category_id": "berries",
        "name": "Berries",
        "icon": "🫐",
        "keywords": [
            "blueberries", "cherry", "grapes", "raspberries", "strawberries",
        ]
    },
    {
        "category_id": "business",
        "name": "Business",
        "icon": "💼",
        "keywords": [
            "sticky notes"
        ]
    },
    {
        "category_id": "canned-food",
        "name": "Canned Food",
        "icon": "🥫",
        "keywords": [
            "beans", "canned tomatoes", "chickpeas", "coconut milk", "gherkins", "jalapenos",
            "olives", "sweetcorn",
            "tuna"
        ]
    },
    {
        "category_id": "car",
        "name": "Car",
        "icon": "🚗",
        "keywords": [
            "gas", "screenwash", "washing"
        ]
    },
    {
        "category_id": "cleaning",
        "name": "Cleaning",
        "icon": "🧹",
        "keywords": [
            "cleaning liquid", "domestos blue", "fairy liquid", "gloves", "hairspray", "kitchen towel",
            "oven cleaning", "scourer", "sponge", "spray", "tissue paper",
            "toilet cleaners", "toilet paper", "washing liquid", "washing powder", "window cleaner", "wipes",
        ]
    },
    {
        "category_id": "clothes",
        "name": "Clothes",
        "icon": "👗",
        "keywords": [
            "baby clothes", "belt", "blouse", "clothes", "dress", "pullover",
            "shirts", "shorts", "socks", "trousers", "underwear"
        ]
    },
    {
        "category_id": "coffee",
        "name": "Coffee",
        "icon": "☕",
        "keywords": [
            "coffee", "coffee drink", "coffee filter", "coffee syrup"
        ]
    },
    {
        "category_id": "cooking-wine",
        "name": "Cooking Wine",
        "icon": "🍾",
        "keywords": [
            "chinese wine"
        ]
    },
    {
        "category_id": "dairy",
        "name": "Dairy",
        "icon": "🧀",
        "keywords": [
            "almond milk", "ayran", "butter", "buttermilk", "cheese",
            "cream", "custard cream", "double cream", "eggs", "hazelnut milk", "ice cream",
            "kefir", "milk", "milk drink", "nut milk", "single cream", "soured cream",
            "soya milk", "vanilla cream", "whipped cream", "yogurt"
        ]
    },
    {
        "category_id": "dishwasher",
        "name": "Dishwasher",
        "icon": "🫧",
        "keywords": [
            "dishwasher salt", "rinse aid"
        ]
    },
    {
        "category_id": "eat-out",
        "name": "Eat Out",
        "icon": "🍽️",
        "keywords": [
            "food"
        ]
    },
    {
        "category_id": "entertainment",
        "name": "Entertainment",
        "icon": "🎲",
        "keywords": [
            "books", "gambling"
        ]
    },
    {
        "category_id": "fish",
        "name": "Fish",
        "icon": "🐟",
        "keywords": [
            "anchovies", "caviar", "cod", "fish", "haddock",
            "lobster", "mackerel", "mussels", "rainbow trout", "salmon",
            "sardines", "sea bass", "sea bream", "seafood sticks", "shrimp",
            "smoked fish", "smoked salmon", "tilapia", "trout"
        ]
    },
    {
        "category_id": "frozen-food",
        "name": "Frozen Food",
        "icon": "🧊",
        "keywords": [
            "chips", "dumplings", "fish fingers", "french fries", "frozen fruit",
            "king prawn", "mixed fruits", "onion rings", "parantha", "pastries", "peas",
            "prawn", "prawns", "shana paratha", "spring rolls", "squid rings",
            "vegetable", 
        ]
    },
    {
        "category_id": "fruits",
        "name": "Fruits",
        "icon": "🍎",
        "keywords": [
            "apples", "apricot", "apricots", "bananas",
            "grapefruit", "kiwi", "lemons", "lychee", "mandarines",
            "mango", "melon", "nectarine", "orange", "oranges", "peach",
            "pears", "physalis", "pineapple", "plum", "plums", "pomegrade",
            "pomegrate", "watermelon"
        ]
    },
    {
        "category_id": "gift",
        "name": "Gift",
        "icon": "🎁",
        "keywords": [
            "card", "flowers", "greeting card"
        ]
    },
    {
        "category_id": "greens",
        "name": "Greens",
        "icon": "🥬",
        "keywords": [
            "babyleaf", "basil", "cavolo nero", "chillies", "chives",
            "coriander", "curry leaves", "dill", "kale", "mint",
            "parsley", "pea shoots", "rosemary", "spinach", "thyme"
        ]
    },
    {
        "category_id": "groceries",
        "name": "Groceries",
        "icon": "🛒",
        "keywords": [
            "hotpot cailiao",
        ]
    },
    {
        "category_id": "health",
        "name": "Health",
        "icon": "🧴",
        "keywords": [
            "bodywash", "cosmetics", "cotton pad", "deodorant", "gillette blades",
            "hand cream", "handwash", "lotion", "make up", "medicine", "menstrual wings",
            "mouthwash", "nail polisher", "nail things", "nasal spray", "razors", "shampoo",
            "shaving blade", "shaving cream", "soap", "toothbrush", "toothbrush parts", "toothpaste",
            "vitamins"
        ]
    },
    {
        "category_id": "healthcare",
        "name": "Healthcare",
        "icon": "💊",
        "keywords": [
            "eyesight", "plasters"
        ]
    },
    {
        "category_id": "house",
        "name": "House",
        "icon": "🏠",
        "keywords": [
            "angle bracket", "bamex mixer", "bed sheet", "bowl", "bungee cord",
            "candle", "candles", "coaster", "cooking dish", "cup",
            "cutlery", "decoration", "french press", "furniture",
            "glasses", "glove oven", "hangers", "house things", "ice tray", "iron board",
            "kitchen outils", "kitchenaid mixer", "kitchenware", "lamp", "marker", "microwave",
            "mixer", "mug", "paper cups", "pillows", "plant", "plastic bags",
            "plates", "printing paper", "ruler", "scissors", "screws",
            "single bbq", "slippers", "sewing thread", "table mats", "thermos",
            "toilet brush", "toilet refreshener", "wine pourers", "wooden cutlery set"
        ]
    },
    {
        "category_id": "juice",
        "name": "Juice",
        "icon": "🧃",
        "keywords": [
            "apple juice", "innocent juice", "orange juice", "smoothie"
        ]
    },
    {
        "category_id": "meat",
        "name": "Meat",
        "icon": "🥩",
        "keywords": [
            "bacon", "beef", "chicken", "duck", "duck legs",
            "garlic sausage", "goat", "ham", "lamb", "meat",
            "pork", "salami", "sausage"
        ]
    },
    {
        "category_id": "oil",
        "name": "Oil",
        "icon": "🫙",
        "keywords": [
            "avocado oil", "beef oil", "duck fat", "goose fat", "lard", "olive oil",
            "other oil", "peanut oil", "pork oil", "rapseed oil", "sesame oil",
            "sunflower oil", "truffle oil", "vegetable oil"
        ]
    },
    {
        "category_id": "others",
        "name": "Others",
        "icon": "📦",
        "keywords": [
            "aluminium foil", "aussie contnr", "bags", "baking bags", "baking paper", "balloon",
            "batteries", "brita mxtra", "cereals", "diffuser",
            "food bags", "golden syrup", "hotpot balls", "ice cubes", "membership renewal", "mineral water",
            "plastic plates", "roasting bags", "rope", "seeds",
            "sparkling water", "tea", "trashbag", "vinegar", "water", "water filter",
            "wrap film", "wrapping paper"
        ]
    },
    {
        "category_id": "porridge",
        "name": "Porridge",
        "icon": "🥣",
        "keywords": [
            "oats"
        ]
    },
    {
        "category_id": "prepared-food",
        "name": "Prepared Food",
        "icon": "🍱",
        "keywords": [
            "breaded chicken", "breaded fish", "chicken wrap",
            "fishcake", "korma", "pate", "pizza", "potato croquettes", "sausage roll",
            "turkey"
        ]
    },
    {
        "category_id": "salad",
        "name": "Salad",
        "icon": "🥗",
        "keywords": [
            "lettuce", "rocket", "salad", "wild rocket"
        ]
    },
    {
        "category_id": "sauce",
        "name": "Sauce",
        "icon": "🫙",
        "keywords": [
            "balsamic sauce", "barbecue sauce", "bbq sauce", "bean sauce", "black bean sauce", "dips",
            "egg mayo sauce", "garlic sauce", "greek sauce", "gruziya sauce", "harissa", "harissa paste",
            "honey", "hot sauce", "hotpot sauce", "jam", "jamaican sauce", "ketchup",
            "marinade", "mayo", "mustard", "oyster sauce", "pesto", "pesto sauce",
            "red pesto", "sauce", "siracha", "soup mix", "soy sauce", "soya sauce",
            "spicy sauce", "syrup", "tahini", "tahini paste", "tomato puree",
            "tartare", "thai paste", "thai sauce", "tomato pasta", "tomato sauce", "worcester sauce",
            "yutaka sauce"
        ]
    },
    {
        "category_id": "snacks",
        "name": "Snacks",
        "icon": "🍿",
        "keywords": [
            "antipasti platter", "biscuits", "bread sticks", "cake", 
            "chocolate", "cookies", "cracker", "crackers", "crisps", 
            "danish pastry", "desert", "doughnut", "eclairs", "hotpot fast food", 
            "jelly", "kitkat", "madeleines", "mikado", "mints", "muffin",
            "muffins", "nuts", "peanuts", "pistachios", "popcorn",
            "pringles", "pudding", "rice ball", "shortbread", "spearmint", "sunflower seeds",
        ]
    },
    {
        "category_id": "soft-drinks",
        "name": "Soft Drinks",
        "icon": "🥤",
        "keywords": [
            "coke", "dr pepper", "energy drink", "iron bru", "kombucha", "kombusha",
            "lemon&turmeric", "lemonade", "monster", "vimto"
        ]
    },
    {
        "category_id": "spice",
        "name": "Spice",
        "icon": "🌶️",
        "keywords": [
            "black pepper", "bread crumbs", "cajun seasoning", "cayenne pepper", "chilli paste", "chips spices",
            "curry", "curry powder", "doujiang", "garlic granules", "garlic paste",
            "ginger paste", "ground coriander", "ground cumin", "ground masala", "hot spice", "hotpot spice",
            "mixed spice", "mustard seeds", "onion granules", "oregano", "paprika",
            "saffron", "salt", "smoked paprika", "spices mix",
            "sriracha", "stock", "sugar", "toban djan", "turmeric", "white pepper",
        ]
    },
    {
        "category_id": "starchy-food",
        "name": "Starchy Food",
        "icon": "🍞",
        "keywords": [
            "boulgur", "bread", "corn flour",
            "croissant", "falafel mix", "flour", "noodles",
            "pain au chocolat", "pasta", "rice", "taco", "wrap"
        ]
    },
    {
        "category_id": "vegetable",
        "name": "Vegetable",
        "icon": "🥦",
        "keywords": [
            "asparagus", "aubergine", "avocados",
            "beetroot", "broccoli", "brussel sprouts", "butternut squash", "cabbage", "carrots",
            "cauliflower", "celery", "corn",
            "courgette", "cucumber", "garlic", "garlic root", "ginger", "green lentils",
            "kidney beans", "leeks", "lentils", "lime",
            "lotus roots", "mushrooms", "okra", "onion",
            "pak choi", "parsnip",
            "peppers", "potatoes", "pumpkin", "radish",
            "red pepper", "shallots", "spring onion", "squash", "swede",
            "sweet potatoes", "tofu", "tomatoes", "turnip", "vegetable selection"
        ]
    },
]


# =============================================================================
# UK RETAILERS
# =============================================================================
RETAILERS = [
    {
        "retailer_id": "tesco",
        "name": "Tesco",
        "aliases": ["tesco stores", "tesco express", "tesco metro", "tesco extra"],
        "header_patterns": ["tesco", "www.tesco.com"],
        "strip_prefixes": [
            "tesco finest", "tesco everyday value", "tesco free from",
            "tesco plant chef", "tesco healthy living", "tesco organic"
        ],
        "skip_patterns": ["clubcard", "saving", "points", "bag for life", "carrier bag"]
    },
    {
        "retailer_id": "sainsburys",
        "name": "Sainsbury's",
        "aliases": ["j sainsbury", "sainsburys local", "sainsbury's"],
        "header_patterns": ["sainsbury", "www.sainsburys.co.uk"],
        "strip_prefixes": [
            "sainsbury's taste the difference", "sainsbury's so organic",
            "sainsbury's love your gut", "sainsbury's freefrom", "by sainsbury's"
        ],
        "skip_patterns": ["nectar", "points", "bag charge"]
    },
    {
        "retailer_id": "asda",
        "name": "ASDA",
        "aliases": ["asda stores", "asda george"],
        "header_patterns": ["asda", "www.asda.com"],
        "strip_prefixes": [
            "asda extra special", "asda good & balanced",
            "asda organic", "chosen by you"
        ],
        "skip_patterns": ["rollback", "savings", "bag charge", "asda rewards"]
    },
    {
        "retailer_id": "morrisons",
        "name": "Morrisons",
        "aliases": ["wm morrison", "morrisons daily"],
        "header_patterns": ["morrisons", "www.morrisons.com"],
        "strip_prefixes": ["morrisons the best", "morrisons savers", "nutmeg"],
        "skip_patterns": ["more card", "points", "bag charge"]
    },
    {
        "retailer_id": "waitrose",
        "name": "Waitrose",
        "aliases": ["waitrose & partners", "waitrose express"],
        "header_patterns": ["waitrose", "www.waitrose.com"],
        "strip_prefixes": [
            "waitrose essential", "waitrose duchy organic",
            "waitrose cooks ingredients", "no.1"
        ],
        "skip_patterns": ["mywaitrose", "partner discount", "bag charge"]
    },
    {
        "retailer_id": "lidl",
        "name": "Lidl",
        "aliases": ["lidl uk", "lidl gb"],
        "header_patterns": ["lidl", "www.lidl.co.uk"],
        "strip_prefixes": ["lidl", "deluxe", "birchwood", "freshona"],
        "skip_patterns": ["coupon", "voucher"]
    },
    {
        "retailer_id": "aldi",
        "name": "Aldi",
        "aliases": ["aldi stores", "aldi uk"],
        "header_patterns": ["aldi", "www.aldi.co.uk"],
        "strip_prefixes": [
            "specially selected", "cowbelle", "nature's pick", "just essentials"
        ],
        "skip_patterns": ["coupon", "voucher", "bag charge"]
    },
    {
        "retailer_id": "marks-spencer",
        "name": "M&S",
        "aliases": ["marks & spencer", "marks and spencer", "m&s simply food"],
        "header_patterns": ["marks & spencer", "m&s", "marksandspencer"],
        "strip_prefixes": ["m&s food", "collection", "gastro pub", "plant kitchen"],
        "skip_patterns": ["sparks", "points", "bag charge"]
    },
    {
        "retailer_id": "coop",
        "name": "Co-op",
        "aliases": ["co-operative", "coop food", "the co-op"],
        "header_patterns": ["co-op", "coop", "co-operative"],
        "strip_prefixes": [
            "co-op irresistible", "co-op truly irresistible",
            "co-op fairtrade", "co-op organic"
        ],
        "skip_patterns": ["member discount", "member price", "dividend", "bag charge"]
    },
    {
        "retailer_id": "iceland",
        "name": "Iceland",
        "aliases": ["iceland foods", "the food warehouse"],
        "header_patterns": ["iceland", "www.iceland.co.uk"],
        "strip_prefixes": ["iceland", "luxury"],
        "skip_patterns": ["bonus card", "points"]
    },
    {
        "retailer_id": "ocado",
        "name": "Ocado",
        "aliases": ["ocado.com", "ocado retail"],
        "header_patterns": ["ocado", "www.ocado.com"],
        "strip_prefixes": ["ocado own"],
        "skip_patterns": ["smart pass", "delivery", "bag charge"]
    },
    {
        "retailer_id": "unknown",
        "name": "Unknown Retailer",
        "aliases": [],
        "header_patterns": [],
        "strip_prefixes": [],
        "skip_patterns": []
    }
]


# =============================================================================
# SEED FUNCTIONS
# =============================================================================

def seed_categories(table):
    print(f"\n  Seeding {len(CATEGORIES)} categories...")
    now = datetime.now(timezone.utc).isoformat()
    with table.batch_writer() as batch:
        for cat in CATEGORIES:
            batch.put_item(Item={**cat, "created_at": now})
            print(f"    {cat['icon']}  {cat['name']}")
    print("  Done.")


def seed_retailers(table):
    print(f"\n  Seeding {len(RETAILERS)} retailers...")
    now = datetime.now(timezone.utc).isoformat()
    with table.batch_writer() as batch:
        for r in RETAILERS:
            batch.put_item(Item={**r, "created_at": now})
            print(f"    {r['name']}")
    print("  Done.")


def seed_item_types(table):
    """
    Seed grocery-item-types table.
    Each keyword becomes a canonical item_type_id pointing to its category.
    Two-hop chain: OCR → item_type_id (MappingsTable) → category_id (ItemTypesTable).
    Changing category_id here retroactively updates all past receipts.
    """
    now   = datetime.now(timezone.utc).isoformat()
    items = []
    seen  = set()
    for cat in CATEGORIES:
        for kw in cat.get("keywords", []):
            item_type_id = kw.lower().strip()
            if item_type_id in seen:
                continue
            seen.add(item_type_id)
            items.append({
                "item_type_id": item_type_id,
                "category_id":  cat["category_id"],
                "created_at":   now
            })

    total = len(items)
    print(f"\n  Seeding {total} item types...")
    with table.batch_writer() as batch:
        for it in items:
            batch.put_item(Item=it)
    print(f"  Done - {total} item types written.")


def seed_mappings(table):
    """
    Seed global keyword->item_type mappings.
    All seeded mappings use store_id = "global" and trust = "trusted".
    mapping_key format: "global#normalized_name"
    Two-hop: mapping stores item_type_id; category resolved via ItemTypesTable.
    """
    now      = datetime.now(timezone.utc).isoformat()
    mappings = []

    for cat in CATEGORIES:
        for kw in cat.get("keywords", []):
            normalized  = kw.lower().strip()
            mapping_key = f"{GLOBAL_STORE}#{normalized}"
            mappings.append({
                "mapping_key":     mapping_key,
                "store_id":        GLOBAL_STORE,
                "normalized_name": normalized,
                "item_type_id":    normalized,   # two-hop: keyword IS the item_type
                "category":        cat["category_id"],  # kept for backwards compat
                "confidence":      "1.00",
                "match_count":     10,
                "trust":           "trusted",
                "source":          "seed",
                "created_at":      now,
                "last_seen":       now
            })

    total = len(mappings)
    print(f"\n  Seeding {total} global keyword mappings (store_id='global')...")
    with table.batch_writer() as batch:
        for m in mappings:
            batch.put_item(Item=m)
    print(f"  Done - {total} mappings written.")
    print(f"  Store-scoped mappings will be built automatically as receipts are scanned.")


def verify_tables(client, env, mappings_table_override=None):
    mappings_name = mappings_table_override or f"grocery-mappings-{env}"
    required = [
        f"grocery-categories-{env}",
        f"grocery-retailers-{env}",
        f"grocery-item-types-{env}",
        mappings_name,
        f"grocery-receipts-{env}",
        f"grocery-items-{env}"
    ]
    print(f"\n  Checking tables exist for environment: {env}")
    existing = client.list_tables()["TableNames"]
    missing  = [t for t in required if t not in existing]
    if missing:
        print(f"\n  ERROR: These tables are missing:")
        for t in missing:
            print(f"    - {t}")
        print(f"\n  Run deploy first:  .\\deploy.ps1 deploy -Env {env}")
        raise SystemExit(1)
    print(f"  All {len(required)} tables found.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",            default="dev", choices=["dev", "prod"])
    parser.add_argument("--region",         default="eu-west-2")
    parser.add_argument("--profile",        default=None)
    parser.add_argument("--mappings-table", default=None,
                        help="Override mappings table name (use when CloudFormation "
                             "generated the name, e.g. grocery-scanner-dev-MappingsTable-XYZ)")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    db      = session.resource("dynamodb", region_name=args.region)
    client  = session.client("dynamodb",   region_name=args.region)

    print("=" * 55)
    print(f"  Grocery Scanner - Seeding Database (Hybrid Mode)")
    print(f"  Environment : {args.env}")
    print(f"  Region      : {args.region}")
    print("=" * 55)

    mappings_table_name = args.mappings_table or f"grocery-mappings-{args.env}"

    verify_tables(client, args.env, mappings_table_override=mappings_table_name)
    seed_categories( db.Table(f"grocery-categories-{args.env}"))
    seed_retailers(  db.Table(f"grocery-retailers-{args.env}"))
    seed_item_types( db.Table(f"grocery-item-types-{args.env}"))
    seed_mappings(   db.Table(mappings_table_name))

    print(f"  Mappings table used: {mappings_table_name}")

    print("\n" + "=" * 55)
    print("  Seed complete! Database is ready.")
    print("  Hybrid matching active (3-layer two-hop):")
    print("    Layer 1 → store-scoped exact  (built as you scan)")
    print("    Layer 2 → in-memory fuzzy/partial (600+ keywords)")
    print("    Layer 3 → flag for review")
    print("  Two-hop: OCR → item_type_id → category (via ItemTypes table)")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()
