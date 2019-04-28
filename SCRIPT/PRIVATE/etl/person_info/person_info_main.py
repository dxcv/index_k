from SCRIPT.PRIVATE.etl.person_info import person_graduate_schoo_linvestment_year, person_info, person_region, \
    person_gender_education, is_core_member


def main():
    person_info.main()
    person_gender_education.main()
    person_region.main()
    person_graduate_schoo_linvestment_year.main()
    is_core_member.main()


if __name__ == "__main__":
    main()
