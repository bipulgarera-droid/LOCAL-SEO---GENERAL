"""
Medical Schema Setup

Instructions:
Run this SQL in Supabase SQL Editor:

-- 1. Medical Projects Table
CREATE TABLE IF NOT EXISTS medical_projects (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    business_name TEXT NOT NULL,
    website TEXT,
    language TEXT DEFAULT 'English',
    location TEXT,
    service_type TEXT,
    phone TEXT,
    address TEXT,
    review_count INTEGER DEFAULT 0,
    average_rating DECIMAL(2,1) DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Google Reviews Table
CREATE TABLE IF NOT EXISTS google_reviews (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    project_id UUID REFERENCES medical_projects(id) ON DELETE CASCADE,
    reviewer_name TEXT,
    star_rating INTEGER CHECK (star_rating >= 1 AND star_rating <= 5),
    review_date DATE,
    review_text TEXT,
    response TEXT,
    response_generated_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Indexes
CREATE INDEX IF NOT EXISTS idx_google_reviews_project_id ON google_reviews(project_id);

-- 4. RLS Policies
ALTER TABLE medical_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_reviews ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on medical_projects" ON medical_projects FOR ALL USING (true);
CREATE POLICY "Allow all on google_reviews" ON google_reviews FOR ALL USING (true);
"""
