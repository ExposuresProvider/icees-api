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
        ("AsthmaDx", Integer, boolean_levels, "Disease"),
        ("CroupDx", Integer, boolean_levels, "Disease"),
        ("ReactiveAirwayDx", Integer, boolean_levels, "Disease"),
        ("CoughDx", Integer, boolean_levels, "Disease"),
        ("PneumoniaDx", Integer, boolean_levels, "Disease"),
        ("ObesityDx", Integer, boolean_levels, "Disease"),
        ("UterineCancerDx", Integer, boolean_levels, "Disease"),
        ("CervicalCancerDx", Integer, boolean_levels, "Disease"),
        ("OvarianCancerDx", Integer, boolean_levels, "Disease"),
        ("ProstateCancerDx", Integer, boolean_levels, "Disease"),
        ("TesticularCancerDx", Integer, boolean_levels, "Disease"),
        ("KidneyCancerDx", Integer, boolean_levels, "Disease"),
        ("EndometriosisDx", Integer, boolean_levels, "Disease"),
        ("OvarianDysfunctionDx", Integer, boolean_levels, "Disease"),
        ("TesticularDysfunctionDx", Integer, boolean_levels, "Disease"),
        ("PregnancyDx", Integer, boolean_levels, "PhenotypicFeature"),
        ("MenopauseDx", Integer, boolean_levels, "PhenotypicFeature"),
        ("DiabetesDx", Integer, boolean_levels, "Disease"),
        ("AlopeciaDx", Integer, boolean_levels, "Disease"),
        ("FibromyalgiaDx", Integer, boolean_levels, "Disease"),
        ("AlcoholDependenceDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DrugDependenceDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DepressionDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AnxietyDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AutismDx", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityBMI", Integer, boolean_levels, "DiseaseOrPhenotypicFeature") ] + 
    [
        (feature_name, Integer, quintile_levels, "ChemicalSubstance") for feature_name in envfeature_names ] + 
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
        ("TotalEDInpatientVisits", Integer, None, "ActivityAndBehavior"),
        ("Prednisone", Integer, boolean_levels, "Drug"),
        ("Fluticasone", Integer, boolean_levels, "Drug"),
        ("Mometasone", Integer, boolean_levels, "Drug"),
        ("Budesonide", Integer, boolean_levels, "Drug"),
        ("Beclomethasone", Integer, boolean_levels, "Drug"),
        ("Ciclesonide", Integer, boolean_levels, "Drug"),
        ("Flunisolide", Integer, boolean_levels, "Drug"),
        ("Albuterol", Integer, boolean_levels, "Drug"),
        ("Metaproterenol", Integer, boolean_levels, "Drug"),
        ("Diphenhydramine", Integer, boolean_levels, "Drug"),
        ("Fexofenadine", Integer, boolean_levels, "Drug"),
        ("Cetirizine", Integer, boolean_levels, "Drug"),
        ("Ipratropium", Integer, boolean_levels, "Drug"),
        ("Salmeterol", Integer, boolean_levels, "Drug"),
        ("Arformoterol", Integer, boolean_levels, "Drug"),
        ("Formoterol", Integer, boolean_levels, "Drug"),
        ("Indacaterol", Integer, boolean_levels, "Drug"),
        ("Theophylline", Integer, boolean_levels, "Drug"),
        ("Omalizumab", Integer, boolean_levels, "Drug"),
        ("Mepolizumab", Integer, boolean_levels, "Drug")
    ],
    "visit": [
        ("VisitType", String, None, "ActivityAndBehavior"),
        ("AgeVisit", age_bins, age_levels, "PhenotypicFeature"),
        ("Sex", sex_bins, sex_levels, "PhenotypicFeature"),
        ("Race", String, None, "PhenotypicFeature"),
        ("Ethnicity", String, None, "PhenotypicFeature"),
        ("AsthmaDxVisit", Integer, boolean_levels, "Disease"),
        ("CroupDxVisit", Integer, boolean_levels, "Disease"),
        ("ReactiveAirwayDxVisit", Integer, boolean_levels, "Disease"),
        ("CoughDxVisit", Integer, boolean_levels, "Disease"),
        ("PneumoniaDxVisit", Integer, boolean_levels, "Disease"),
        ("ObesityDxVisit", Integer, boolean_levels, "Disease"),
        ("UterineCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("CervicalCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("OvarianCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("ProstateCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("TesticularCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("KidneyCancerDxVisit", Integer, boolean_levels, "Disease"),
        ("EndometriosisDxVisit", Integer, boolean_levels, "Disease"),
        ("OvarianDysfunctionDxVisit", Integer, boolean_levels, "Disease"),
        ("TesticularDysfunctionDxVisit", Integer, boolean_levels, "Disease"),
        ("PregnancyDxVisit", Integer, boolean_levels, "PhenotypicFeature"),
        ("MenopauseDxVisit", Integer, boolean_levels, "PhenotypicFeature"),
        ("DiabetesDxVisit", Integer, boolean_levels, "Disease"),
        ("AlopeciaDxVisit", Integer, boolean_levels, "Disease"),
        ("FibromyalgiaDxVisit", Integer, boolean_levels, "Disease"),
        ("AlcoholDependenceDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DrugDependenceDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("DepressionDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AnxietyDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("AutismDxVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("ObesityBMIVisit", Integer, boolean_levels, "DiseaseOrPhenotypicFeature"),
        ("Avg24hPM2.5Exposure", Integer, quintile_levels, "ChemicalSubstance"),
        ("Max24hPM2.5Exposure", Integer, quintile_levels, "ChemicalSubstance"),
        ("Avg24hOzoneExposure", Integer, quintile_levels, "ChemicalSubstance"),
        ("Max24hOzoneExposure", Integer, quintile_levels, "ChemicalSubstance"),
        ("Avg24hPM2.5Exposure_qcut", Integer, quintile_levels, "ChemicalSubstance"),
        ("Max24hPM2.5Exposure_qcut", Integer, quintile_levels, "ChemicalSubstance"),
        ("Avg24hOzoneExposure_qcut", Integer, quintile_levels, "ChemicalSubstance"),
        ("Max24hOzoneExposure_qcut", Integer, quintile_levels, "ChemicalSubstance"),
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
        ("PrednisoneVisit", Integer, boolean_levels, "Drug"),
        ("FluticasoneVisit", Integer, boolean_levels, "Drug"),
        ("MometasoneVisit", Integer, boolean_levels, "Drug"),
        ("BudesonideVisit", Integer, boolean_levels, "Drug"),
        ("BeclomethasoneVisit", Integer, boolean_levels, "Drug"),
        ("CiclesonideVisit", Integer, boolean_levels, "Drug"),
        ("FlunisolideVisit", Integer, boolean_levels, "Drug"),
        ("AlbuterolVisit", Integer, boolean_levels, "Drug"),
        ("MetaproterenolVisit", Integer, boolean_levels, "Drug"),
        ("DiphenhydramineVisit", Integer, boolean_levels, "Drug")
        ("FexofenadineVisit", Integer, boolean_levels, "Drug"),
        ("CetirizineVisit", Integer, boolean_levels, "Drug"),
        ("IpratropiumVisit", Integer, boolean_levels, "Drug"),
        ("SalmeterolVisit", Integer, boolean_levels, "Drug"),
        ("ArformoterolVisit", Integer, boolean_levels, "Drug")
        ("FormoterolVisit", Integer, boolean_levels, "Drug"),
        ("IndacaterolVisit", Integer, boolean_levels, "Drug"),
        ("TheophyllineVisit", Integer, boolean_levels, "Drug"),
        ("OmalizumabVisit", Integer, boolean_levels, "Drug"),
        ("MepolizumabVisit", Integer, boolean_levels, "Drug")
    ]
}

def lookUpFeatureClass(table, feature):
    for n, _, _, c in features[table]:
        if n == feature:
            return C
    return None
