# SEO Agency SOP & Philosophy

## The Bottom-Up Strategy
- **Focus**: Commercial intent over informational intent.
- **Flow**: Bottom Funnel (Product/Service) -> Middle Funnel (Comparison/Best Of) -> Top Funnel (Informational).
- **Linking**: Top links to Middle, Middle links to Bottom.

## Architecture
1.  **Tech Audit**: Scans all URLs for status codes, meta tags, speed, etc.
2.  **URL Classification**: Identifies BoFu, MoFu, ToFu pages.
3.  **Context Engineering**:
    -   **Business Overview**: Generated first. Defines what the business does, ICP, brand voice.
    -   **Content Strategy**: Generated second. Defines the overall plan based on the Business Overview.
4.  **Content Generation**: Uses the Context (Overview + Strategy) + Research (Perplexity/DataForSEO) to write articles.

## Workflow
1.  **Project Dashboard**: Define Company, URL, Language, Location, Focus.
2.  **Start**: Triggers Business Overview & Strategy generation.
3.  **Tech Audit**: Checks for technical issues.
4.  **Classification**: Manual/AI classification of pages.
5.  **Strategy**: Generate content ideas based on the funnel.

## Tech Stack
- **Backend**: Python/Flask (replacing n8n for this implementation).
- **Database**: Supabase.
- **AI**: Gemini (Context/Writing), Perplexity (Research).
- **Frontend**: HTML/Tailwind (Airtable-style UI).
