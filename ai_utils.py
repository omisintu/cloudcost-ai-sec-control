import os
from openai import OpenAI
from dotenv import load_dotenv
import logging
import hashlib
from sqlalchemy import text
from db import engine
import time
load_dotenv()

encryptedKey = "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"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def enhance_with_ai(title, description, impact):
    time.sleep(1.2)  # prevent rate burst


def enhance_with_ai(title, description, impact):
        """
        Existing code : Convert structured insight into polished human-readable explanation.
        """

        try:
            prompt = f"""
            You are a FinOps AI assistant.

            Rewrite the following cloud cost insight in a professional, concise, and actionable way.

            Rules:
            - Keep it under 80 words
            - Be clear and business-friendly
            - Explain WHY it happened
            - Suggest WHAT to do
            - Do NOT hallucinate unknown data

            Input:
            Title: {title}
            Description: {description}
            Impact: {impact}

            Output:
            """

            response = client.chat.completions.create(
                model="gpt-4.1-mini",  # cost-efficient + fast
                messages=[
                    {"role": "system", "content": "You are a cloud cost optimization expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,  # deterministic
                max_tokens=120
            )

            airesponseText = response.choices[0].message.content.strip()
            
        except Exception as e:
            if "insufficient_quota" in str(e):
                logging.warning("Quota exceeded, skipping AI enhancement")
            else:
                logging.error(f"AI failed: {e}")

            airesponseText = f"{title}. {description} {impact}"
    
        raw_input = f"{title}-{description}-{impact}"
        key = hashlib.sha256(raw_input.encode()).hexdigest()

        cached = get_cached_response(key)
        if cached:
            return cached

        # call OpenAI
        output = airesponseText

        store_cache(key, output)
        return output




def get_cached_response(key):
    query = text("SELECT output FROM ai_cache WHERE input_hash = :key")

    with engine.connect() as conn:
        result = conn.execute(query, {"key": key}).fetchone()

    return result[0] if result else None


def store_cache(key, value):
    query = text("""
        INSERT INTO ai_cache (input_hash, output)
        VALUES (:key, :value)
        ON CONFLICT (input_hash) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(query, {"key": key, "value": value})