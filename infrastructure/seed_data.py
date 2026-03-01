#!/usr/bin/env python3
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
        "category_id": "vegetables",
        "name": "Vegetables",
        "icon": "🥦",
        "keywords": [
            "asparagus", "aubergine", "avocado", "babyleaf", "beetroot",
            "broccoli", "brussel sprouts", "butternut squash", "cabbage",
            "carrot", "cauliflower", "cavolo nero", "celery", "chilli",
            "corn", "courgette", "cucumber", "fennel", "garlic",
            "ginger", "green lentils", "kale", "kidney beans", "leek",
            "lentils", "lotus root", "mushroom", "okra", "onion",
            "pak choi", "parsnip", "pea shoots", "pepper", "potato",
            "pumpkin", "radish", "red pepper", "shallot", "spinach",
            "spring onion", "squash", "swede", "sweet potato", "sweetcorn",
            "tofu", "tomato", "turnip", "vegetable mix", "vine tomatoes",
            "cherry tomatoes", "watercress",
        ]
    },
    {
        "category_id": "fruits",
        "name": "Fruits",
        "icon": "🍎",
        "keywords": [
            "apple", "apricot", "banana", "blackberry", "blueberry",
            "cherry", "clementine", "grape", "grapefruit", "kiwi",
            "lemon", "lime", "lychee", "mandarin", "mango", "melon",
            "nectarine", "orange", "peach", "pear", "physalis",
            "pineapple", "plum", "pomegranate", "raspberry", "satsuma",
            "strawberry", "watermelon",
        ]
    },
    {
        "category_id": "salad-herbs",
        "name": "Salad & Herbs",
        "icon": "🌿",
        "keywords": [
            "basil", "chives", "coriander", "curry leaves", "dill",
            "mint", "parsley", "rocket", "rosemary", "salad", "thyme",
            "wild rocket",
        ]
    },
    {
        "category_id": "meat",
        "name": "Meat",
        "icon": "🥩",
        "keywords": [
            "bacon", "beef", "beef mince", "chicken", "chicken breast",
            "chicken thigh", "chicken wings", "chorizo", "duck", "duck legs",
            "gammon", "garlic sausage", "goat", "ham", "lamb", "lamb chops",
            "lamb mince", "pancetta", "pepperoni", "pork", "pork belly",
            "pork chop", "prosciutto", "salami", "sausage", "steak",
            "turkey", "veal", "venison", "breaded chicken",
        ]
    },
    {
        "category_id": "fish-seafood",
        "name": "Fish & Seafood",
        "icon": "🐟",
        "keywords": [
            "anchovy", "caviar", "clam", "cod", "crab", "fish cake",
            "fish fingers", "haddock", "lobster", "mackerel", "mussel",
            "oyster", "plaice", "prawn", "rainbow trout", "salmon",
            "sardine", "scallop", "sea bass", "sea bream", "seafood sticks",
            "shrimp", "smoked fish", "smoked salmon", "squid", "tilapia",
            "trout", "tuna", "king prawn", "squid rings", "breaded fish",
            "fishcake",
        ]
    },
    {
        "category_id": "dairy-eggs",
        "name": "Dairy & Eggs",
        "icon": "🧀",
        "keywords": [
            "almond milk", "buttermilk", "hazelnut milk", "milk",
            "nut milk", "oat milk", "semi-skimmed", "skimmed milk",
            "soya milk", "whole milk",
            "brie", "camembert", "cheddar", "cheese", "cottage cheese",
            "cream cheese", "feta", "gouda", "halloumi", "mozzarella",
            "parmesan", "stilton",
            "ayran", "fromage frais", "greek yogurt", "kefir",
            "yoghurt", "yogurt",
            "butter", "creme fraiche", "custard", "double cream",
            "lurpak", "single cream", "soured cream", "vanilla cream",
            "whipped cream", "whipping cream",
            "egg", "eggs", "free range eggs",
        ]
    },
    {
        "category_id": "bakery",
        "name": "Bakery & Bread",
        "icon": "🍞",
        "keywords": [
            "baguette", "bagel", "bap", "biscuit", "bread", "brownie",
            "cake", "ciabatta", "cracker", "croissant", "crumpet",
            "danish", "doughnut", "flapjack", "hot cross bun", "hovis",
            "kingsmill", "loaf", "muffin", "naan", "oatcake",
            "pain au chocolat", "pitta", "roll", "rye bread", "scone",
            "sourdough", "tortilla", "warburtons", "white bread",
            "wholemeal", "wrap",
        ]
    },
    {
        "category_id": "frozen",
        "name": "Frozen",
        "icon": "❄️",
        "keywords": [
            "chips", "dumplings", "fish fingers", "french fries",
            "frozen berries", "frozen fruit", "frozen peas", "frozen pizza",
            "frozen veg", "garlic bread", "hash brown", "ice cream",
            "ice lolly", "mixed fruits", "onion rings", "paratha",
            "pizza", "sorbet", "spring rolls", "chicken nugget",
        ]
    },
    {
        "category_id": "drinks",
        "name": "Drinks",
        "icon": "🥤",
        "keywords": [
            "apple juice", "coffee", "coffee drink", "coffee filter",
            "coke", "cola", "cordial", "dr pepper", "energy drink",
            "fanta", "fruit juice", "green tea", "herbal tea",
            "hot chocolate", "innocent", "innocent juice", "iron bru",
            "kombucha", "lemonade", "lucozade", "mineral water", "monster",
            "orange juice", "ribena", "smoothie", "sparkling water",
            "sprite", "squash", "still water", "tea", "tropicana",
            "vimto", "water",
        ]
    },
    {
        "category_id": "alcohol",
        "name": "Alcohol",
        "icon": "🍷",
        "keywords": [
            "ale", "armagnac", "beer", "champagne", "cider", "cocktail",
            "cognac", "cointreau", "gin", "grappa", "lager", "limoncello",
            "liqueur", "prosecco", "red wine", "rose wine", "rum",
            "spirits", "vodka", "whiskey", "whisky", "white wine", "wine",
        ]
    },
    {
        "category_id": "pasta-rice",
        "name": "Pasta & Rice",
        "icon": "🍝",
        "keywords": [
            "boulgur", "corn flour", "couscous", "falafel mix", "flour",
            "noodle", "oats", "pasta", "porridge oats", "quinoa",
            "rice", "spaghetti", "taco",
        ]
    },
    {
        "category_id": "tinned-jarred",
        "name": "Tinned & Jarred",
        "icon": "🥫",
        "keywords": [
            "baked beans", "beans", "canned tomatoes", "chickpeas",
            "chopped tomatoes", "coconut milk", "gherkins", "jalapenos",
            "kidney beans", "lentils", "olives", "peas", "sweetcorn",
            "tinned tomatoes", "tuna",
        ]
    },
    {
        "category_id": "sauces-condiments",
        "name": "Sauces & Condiments",
        "icon": "🫙",
        "keywords": [
            "balsamic sauce", "barbecue sauce", "bbq sauce", "bean sauce",
            "black bean sauce", "dips", "egg mayo sauce", "garlic sauce",
            "harissa", "harissa paste", "honey", "hot sauce", "jam",
            "ketchup", "marinade", "marmalade", "mayo", "mayonnaise",
            "mustard", "oyster sauce", "peanut butter", "pesto",
            "red pesto", "sauce", "sriracha", "soy sauce", "stock",
            "syrup", "tahini", "tomato puree", "tomato sauce",
            "vinegar", "worcester sauce",
        ]
    },
    {
        "category_id": "oils-fats",
        "name": "Oils & Fats",
        "icon": "🫒",
        "keywords": [
            "avocado oil", "coconut oil", "duck fat", "goose fat",
            "lard", "olive oil", "peanut oil", "rapeseed oil",
            "sesame oil", "sunflower oil", "truffle oil", "vegetable oil",
        ]
    },
    {
        "category_id": "baking",
        "name": "Baking",
        "icon": "🧁",
        "keywords": [
            "baking powder", "breadcrumbs", "cacao", "cinnamon",
            "food colour", "sugar", "vanilla", "yeast",
        ]
    },
    {
        "category_id": "spices",
        "name": "Spices",
        "icon": "🌶️",
        "keywords": [
            "black pepper", "cajun seasoning", "cayenne pepper",
            "chilli paste", "curry powder", "garlic granules",
            "garlic paste", "ginger paste", "ground coriander",
            "ground cumin", "ground masala", "mixed spice", "mustard seeds",
            "onion granules", "oregano", "paprika", "saffron", "salt",
            "smoked paprika", "spice", "turmeric", "white pepper",
        ]
    },
    {
        "category_id": "snacks",
        "name": "Snacks & Confectionery",
        "icon": "🍫",
        "keywords": [
            "almond", "antipasti platter", "biscuits", "bread sticks",
            "cashew", "cereal bar", "chocolate", "cookies", "crackers",
            "crisps", "dairy milk", "danish pastry", "dessert",
            "doughnut", "doritos", "eclairs", "haribo", "jelly",
            "kit kat", "kitkat", "madeleines", "mars", "mikado",
            "mints", "mixed nuts", "muffin", "nakd", "nuts",
            "peanut", "pistachio", "popcorn", "pringles", "protein bar",
            "pudding", "rice ball", "shortbread", "snickers", "spearmint",
            "sunflower seeds", "sweets", "twix", "walnut",
        ]
    },
    {
        "category_id": "ready-meals",
        "name": "Ready Meals & Deli",
        "icon": "🍱",
        "keywords": [
            "chicken wrap", "coleslaw", "curry", "fishcake", "guacamole",
            "hummus", "hotpot", "korma", "meal deal", "pate", "pasty",
            "potato croquettes", "quiche", "ready meal", "sandwich",
            "sausage roll", "sushi",
        ]
    },
    {
        "category_id": "health-beauty",
        "name": "Health & Beauty",
        "icon": "💊",
        "keywords": [
            "bodywash", "cosmetics", "cotton pad", "deodorant",
            "eyesight", "gillette blades", "glasses", "hand cream",
            "hand wash", "handwash", "ibuprofen", "lip balm", "lotion",
            "make up", "medicine", "menstrual wings", "moisturiser",
            "mouthwash", "nail polish", "nasal spray", "paracetamol",
            "plaster", "razors", "shampoo", "shaving blade",
            "shaving cream", "shower gel", "soap", "sunscreen",
            "supplement", "toothbrush", "toothpaste", "vitamin",
        ]
    },
    {
        "category_id": "cleaning",
        "name": "Cleaning",
        "icon": "🧹",
        "keywords": [
            "bin bags", "bleach", "cleaning liquid", "clingfilm",
            "dettol", "dishwasher salt", "dishwasher tablets", "domestos",
            "fabric softener", "fairy", "fairy liquid", "gloves",
            "hairspray", "kitchen foil", "kitchen roll", "kitchen towel",
            "oven cleaner", "rinse aid", "scourer", "sponge", "spray",
            "tissue", "tissue paper", "toilet cleaner", "toilet paper",
            "toilet roll", "washing liquid", "washing powder",
            "washing up", "window cleaner", "wipes",
        ]
    },
    {
        "category_id": "homewares",
        "name": "Homewares",
        "icon": "🏠",
        "keywords": [
            "angle bracket", "bed sheet", "bowl", "bungee cord", "candle",
            "coaster", "cooking dish", "cup", "cutlery", "decoration",
            "french press", "furniture", "hangers", "house things",
            "ice tray", "ironing board", "kitchen utensils", "kitchenware",
            "lamp", "microwave", "mixer", "mug", "paper cups", "pillows",
            "plant", "plastic bags", "plates", "ruler", "scissors",
            "screws", "slippers", "table mats", "thermos", "toilet brush",
            "wine pourers", "wooden cutlery",
        ]
    },
    {
        "category_id": "clothes",
        "name": "Clothing",
        "icon": "👕",
        "keywords": [
            "belt", "blouse", "dress", "pullover", "shirts",
            "shorts", "socks", "trousers", "underwear",
        ]
    },
    {
        "category_id": "baby",
        "name": "Baby & Child",
        "icon": "👶",
        "keywords": [
            "aptamil", "baby bottles", "baby food", "baby latch",
            "baby milk", "baby powder", "baby snack", "baby wipes",
            "formula", "kids pencils", "nappies", "nappy",
            "pampers", "pregnancy test", "straws", "toys", "water wipes",
        ]
    },
    {
        "category_id": "other",
        "name": "Other",
        "icon": "📦",
        "keywords": [
            "aluminium foil", "baking bags", "baking paper", "batteries",
            "cereals", "diffuser", "food bags", "golden syrup",
            "ice cubes", "membership renewal", "plastic plates",
            "roasting bags", "rope", "seeds", "trashbag", "water filter",
            "wrap film",
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


def seed_mappings(table):
    """
    Seed global keyword->category mappings.
    All seeded mappings use store_id = "global" and trust = "trusted".
    mapping_key format: "global#normalized_name"
    """
    now      = datetime.now(timezone.utc).isoformat()
    mappings = []

    for cat in CATEGORIES:
        for kw in cat.get("keywords", []):
            normalized = kw.lower().strip()
            mapping_key = f"{GLOBAL_STORE}#{normalized}"
            mappings.append({
                "mapping_key":     mapping_key,
                "store_id":        GLOBAL_STORE,
                "normalized_name": normalized,
                "category":        cat["category_id"],
                "confidence":      "1.00",
                "match_count":     10,      # Pre-trusted from day one
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
    seed_categories(db.Table(f"grocery-categories-{args.env}"))
    seed_retailers( db.Table(f"grocery-retailers-{args.env}"))
    seed_mappings(  db.Table(mappings_table_name))

    print(f"  Mappings table used: {mappings_table_name}")

    print("\n" + "=" * 55)
    print("  Seed complete! Database is ready.")
    print("  Hybrid matching active:")
    print("    Layer 1 → store-scoped exact  (built as you scan)")
    print("    Layer 2 → global exact        (607 keywords seeded)")
    print("    Layer 3 → in-memory fuzzy/partial")
    print("    Layer 4 → flag for review")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()
