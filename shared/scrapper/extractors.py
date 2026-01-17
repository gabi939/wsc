from bs4 import BeautifulSoup
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple

import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple


def _calculate_complexity_score(
    title: str, full_text: str, requirements: List[str]
) -> int:
    """
    Calculate complexity score (0-100) based on various factors.
    """
    score = 0

    num_requirements = len(requirements)
    skill_score = min(35, num_requirements * 5)
    score += skill_score

    experience_years = _extract_years_experience(full_text)
    if experience_years >= 7:
        score += 30
    elif experience_years >= 5:
        score += 25
    elif experience_years >= 3:
        score += 20
    elif experience_years >= 1:
        score += 10

    seniority_keywords = [
        "lead",
        "senior",
        "principal",
        "staff",
        "architect",
        "director",
        "head",
        "chief",
    ]
    title_lower = title.lower()
    for keyword in seniority_keywords:
        if keyword in title_lower:
            score += 20
            break

    complexity_keywords = [
        "state-of-the-art",
        "cutting-edge",
        "research",
        "phd",
        "deep learning",
        "machine learning",
        "ai",
        "algorithm",
        "architecture",
        "distributed",
        "scalability",
        "real-time",
        "end-to-end",
        "full cycle",
        "generative",
    ]
    complexity_count = sum(1 for keyword in complexity_keywords if keyword in full_text)
    score += min(15, complexity_count * 2)

    return min(100, score)


def _extract_years_experience(text: str) -> int:
    """
    Extract years of experience required from text.
    """
    patterns = [
        r"(\d+)\+?\s*years",
        r"(\d+)\s*-\s*(\d+)\s*years",
    ]

    max_years = 0
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if isinstance(match, tuple):
                years = max(int(y) for y in match if y.isdigit())
            else:
                years = int(match)
            max_years = max(max_years, years)

    return max_years


def _categorize_position(
    title: str, full_text: str, responsibilities: List[str]
) -> str:
    """
    Categorize position into: Engineering, Product, Design, Operations, or Other.
    """
    title_lower = title.lower()
    combined_text = (title + " " + full_text).lower()

    engineering_keywords = [
        "engineer",
        "developer",
        "software",
        "algorithm",
        "backend",
        "frontend",
        "full stack",
        "devops",
        "sre",
        "data engineer",
        "ml",
        "ai",
        "nlp",
        "computer vision",
        "infrastructure",
        "architect",
        "technical",
    ]

    product_keywords = [
        "product manager",
        "product owner",
        "pm",
        "product lead",
        "product strategy",
        "roadmap",
        "stakeholder",
    ]

    design_keywords = [
        "designer",
        "ux",
        "ui",
        "user experience",
        "user interface",
        "graphic design",
        "visual design",
        "design system",
    ]

    operations_keywords = [
        "operations",
        "ops manager",
        "program manager",
        "project manager",
        "scrum master",
        "agile coach",
        "business operations",
        "customer success",
    ]

    if any(keyword in combined_text for keyword in engineering_keywords):
        return "Engineering"
    elif any(keyword in combined_text for keyword in product_keywords):
        return "Product"
    elif any(keyword in combined_text for keyword in design_keywords):
        return "Design"
    elif any(keyword in combined_text for keyword in operations_keywords):
        return "Operations"
    else:
        return "Other"


def _determine_seniority(title: str, full_text: str, requirements: List[str]) -> str:
    """
    Determine seniority level: Junior, Mid, Senior, or Lead.
    """
    title_lower = title.lower()
    combined_text = (title + " " + full_text).lower()

    if any(
        keyword in title_lower
        for keyword in ["lead", "principal", "staff", "director", "head", "chief"]
    ):
        return "Lead"
    elif "senior" in title_lower or "sr." in title_lower:
        return "Senior"
    elif any(
        keyword in title_lower for keyword in ["junior", "jr.", "entry", "associate"]
    ):
        return "Junior"

    years_exp = _extract_years_experience(full_text)

    has_advanced_degree = any(
        keyword in combined_text for keyword in ["phd", "ph.d", "m.sc", "master"]
    )

    has_leadership = any(
        keyword in combined_text
        for keyword in [
            "lead team",
            "mentor",
            "autonomy",
            "independently",
            "ownership",
            "drive",
            "strategy",
            "architecture decision",
        ]
    )

    if years_exp >= 7 or has_advanced_degree and years_exp >= 5:
        return "Lead"
    elif years_exp >= 5 or (years_exp >= 3 and has_leadership):
        return "Senior"
    elif years_exp >= 2 or years_exp >= 1 and len(requirements) <= 5:
        return "Mid"
    else:
        return "Junior"


def extract_job_info(html: str) -> Dict:
    """
    Extract and enrich job posting information from HTML content.

    Args:
        html: HTML string containing job posting

    Returns:
        Dictionary with extracted and enriched job information
    """
    soup = BeautifulSoup(html, "html.parser")

    title = soup.find("h1").text.strip() if soup.find("h1") else ""
    full_text = soup.get_text().lower()

    requirements = []
    requirements_section = soup.find(
        "div", class_="career-text-block__wrp--data--requirements"
    )
    if requirements_section:
        req_items = requirements_section.find_all("li")
        requirements = [item.text.strip() for item in req_items]

    responsibilities = []
    desc_section = soup.find("div", class_="careers-text-block__desc")
    if desc_section:
        resp_items = desc_section.find_all("li")
        responsibilities = [item.text.strip() for item in resp_items]

    complexity_score = _calculate_complexity_score(title, full_text, requirements)

    category = _categorize_position(title, full_text, responsibilities)

    seniority_level = _determine_seniority(title, full_text, requirements)

    return {
        "title": title,
        "complexity_score": complexity_score,
        "category": category,
        "seniority_level": seniority_level,
        "requirements_count": len(requirements),
        "responsibilities_count": len(responsibilities),
        "details": {"requirements": requirements, "responsibilities": responsibilities},
    }


def extract_positions(html: str) -> list[dict[str, str]]:
    """Extract position information from HTML content."""
    soup = BeautifulSoup(html, "html.parser")
    positions = []

    response_jobs_div = soup.find("div", id="response_jobs")

    if response_jobs_div:
        job_links = response_jobs_div.find_all("a")

        for i, link in enumerate(job_links):
            title_element = link.find("span", class_="link-text")
            if title_element:
                title = title_element.get_text(strip=True)
                url = link.get("href", "")
                positions.append(
                    {"position_title": title, "index": str(i), "job_url": url}
                )

    return positions
