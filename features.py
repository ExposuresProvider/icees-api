from sqlalchemy import Integer, String, Enum
age_levels = ['0-2', '3-17', '18-34', '35-50', '51-69', '70+']
age_bins = Enum(*age_levels)
sex_levels = ["M","F"]
sex_bins = Enum(*sex_levels)
ur_levels = ["R","U"]
ur_bins = Enum(*ur_levels)
est_residential_density_levels = range(1, 3)
quartile_levels = range(1, 5)
quintile_levels = range(1, 6)
sextile_levels = range(1, 7)
boolean_levels = [0, 1]


envfeature_names = [
    stat + "Daily" + feature + "Exposure" + suffix + binning for binning in ["", "_qcut"] for feature in ["PM2.5", "Ozone"] for stat in ["Avg", "Max"] for suffix in ["", "_StudyAvg", "_StudyMax"]
]

features = {
    "patient": [
        ("AgeStudyStart", age_bins, age_levels, "PhenotypicFeature"),
        ("Sex", sex_bins, sex_levels, "PhenotypicFeature"),
        ("Race", String, None, "PhenotypicFeature"),
        ("Ethnicity", String, None, "PhenotypicFeature"),
        ("AsthmaDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CroupDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ReactiveAirwayDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CoughDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("PneumoniaDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("UterineCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CervicalCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("OvarianCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ProstateCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("TesticularCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("KidneyCancerDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("EndometriosisDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("OvarianDysfunctionDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("TesticularDysfunctionDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("PregnancyDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("MenopauseDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DiabetesDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AlopeciaDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("FibromyalgiaDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AlcoholDependenceDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DrugDependenceDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DepressionDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AnxietyDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AutismDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityBMI", Integer, boolean_levels, "DiseaseOrPhenotypicFeature") ] + 
    [
        (feature_name, Integer, quintile_levels, "EnvironmentalFeature") for feature_name in envfeature_names ] + 
    [
        ("EstResidentialDensity", Integer, est_residential_density_levels, "EnvironmentalFeature"),
        ("EstResidentialDesnity25Plus", Integer, quintile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNonHispWhite", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityHouseholdNonHispWhite", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityHighSchoolMaxEducation", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNoAuto", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNoHealthIns", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityESL", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstHouseholdIncome", Integer, quintile_levels, "EnvironmentalFeature"),
        ("ur", ur_bins, ur_levels, "EnvironmentalFeature"),
        ("MajorRoadwayHighwayExposure", Integer, sextile_levels, "EnvironmentalFeature"),
        ("TotalEDInpatientVisits", Integer, None, "DrugExposure"),
        ("Prednisone", Integer, boolean_levels, "DrugExposure"),
        ("Fluticasone", Integer, boolean_levels, "DrugExposure"),
        ("Mometasone", Integer, boolean_levels, "DrugExposure"),
        ("Budesonide", Integer, boolean_levels, "DrugExposure"),
        ("Beclomethasone", Integer, boolean_levels, "DrugExposure"),
        ("Ciclesonide", Integer, boolean_levels, "DrugExposure"),
        ("Flunisolide", Integer, boolean_levels, "DrugExposure"),
        ("Albuterol", Integer, boolean_levels, "DrugExposure"),
        ("Metaproterenol", Integer, boolean_levels, "DrugExposure"),
        ("Diphenhydramine", Integer, boolean_levels, "DrugExposure"),
        ("Fexofenadine", Integer, boolean_levels, "DrugExposure"),
        ("Cetirizine", Integer, boolean_levels, "DrugExposure"),
        ("Ipratropium", Integer, boolean_levels, "DrugExposure"),
        ("Salmeterol", Integer, boolean_levels, "DrugExposure"),
        ("Arformoterol", Integer, boolean_levels, "DrugExposure"),
        ("Formoterol", Integer, boolean_levels, "DrugExposure"),
        ("Indacaterol", Integer, boolean_levels, "DrugExposure"),
        ("Theophylline", Integer, boolean_levels, "DrugExposure"),
        ("Omalizumab", Integer, boolean_levels, "DrugExposure"),
        ("Mepolizumab", Integer, boolean_levels, "DrugExposure")
    ],
    "visit": [
        ("VisitType", String, None, "ActivityAndBehavior"),
        ("AgeVisit", age_bins, age_levels, "PhenotypicFeature"),
        ("Sex", sex_bins, sex_levels, "PhenotypicFeature"),
        ("Race", String, None, "PhenotypicFeature"),
        ("Ethnicity", String, None, "PhenotypicFeature"),
        ("AsthmaDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CroupDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ReactiveAirwayDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CoughDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("PneumoniaDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("UterineCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("CervicalCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("OvarianCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ProstateCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("TesticularCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("KidneyCancerDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("EndometriosisDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("OvarianDysfunctionDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("TesticularDysfunctionDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("PregnancyDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("MenopauseDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DiabetesDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AlopeciaDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("FibromyalgiaDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AlcoholDependenceDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DrugDependenceDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DepressionDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AnxietyDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AutismDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityBMIVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("Avg24hPM2.5Exposure", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Max24hPM2.5Exposure", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Avg24hOzoneExposure", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Max24hOzoneExposure", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Avg24hPM2.5Exposure_qcut", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Max24hPM2.5Exposure_qcut", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Avg24hOzoneExposure_qcut", Integer, quintile_levels, "EnvironmentalFeature"),
        ("Max24hOzoneExposure_qcut", Integer, quintile_levels, "EnvironmentalFeature"),
        ("EstResidentialDensity", Integer, quintile_levels, "EnvironmentalFeature"),
        ("EstResidentialDesnity25Plus", Integer, quintile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNonHispWhite", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityHouseholdNonHispWhite", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityHighSchoolMaxEducation", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNoAuto", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityNoHealthIns", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstProbabilityESL", Integer, quartile_levels, "EnvironmentalFeature"),
        ("EstHouseholdIncome", Integer, quintile_levels, "EnvironmentalFeature"),
        ("ur", ur_bins, ur_levels, "EnvironmentalFeature"),
        ("MajorRoadwayHighwayExposure", Integer, sextile_levels, "EnvironmentalFeature"),
        ("PrednisoneVisit", Integer, boolean_levels, "DrugExposure"),
        ("FluticasoneVisit", Integer, boolean_levels, "DrugExposure"),
        ("MometasoneVisit", Integer, boolean_levels, "DrugExposure"),
        ("BudesonideVisit", Integer, boolean_levels, "DrugExposure"),
        ("BeclomethasoneVisit", Integer, boolean_levels, "DrugExposure"),
        ("CiclesonideVisit", Integer, boolean_levels, "DrugExposure"),
        ("FlunisolideVisit", Integer, boolean_levels, "DrugExposure"),
        ("AlbuterolVisit", Integer, boolean_levels, "DrugExposure"),
        ("MetaproterenolVisit", Integer, boolean_levels, "DrugExposure"),
        ("DiphenhydramineVisit", Integer, boolean_levels, "DrugExposure")
        ("FexofenadineVisit", Integer, boolean_levels, "DrugExposure"),
        ("CetirizineVisit", Integer, boolean_levels, "DrugExposure"),
        ("IpratropiumVisit", Integer, boolean_levels, "DrugExposure"),
        ("SalmeterolVisit", Integer, boolean_levels, "DrugExposure"),
        ("ArformoterolVisit", Integer, boolean_levels, "DrugExposure")
        ("FormoterolVisit", Integer, boolean_levels, "DrugExposure"),
        ("IndacaterolVisit", Integer, boolean_levels, "DrugExposure"),
        ("TheophyllineVisit", Integer, boolean_levels, "DrugExposure"),
        ("OmalizumabVisit", Integer, boolean_levels, "DrugExposure"),
        ("MepolizumabVisit", Integer, boolean_levels, "DrugExposure")
    ]
}

def lookUpFeatureClass(table, feature):
    for n, _, _, c in features[table]:
        if n == feature:
            return C
    return None
