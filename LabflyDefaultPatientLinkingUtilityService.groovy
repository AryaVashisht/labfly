import com.mphrx.auth.User
import com.mphrx.util.user.LoggedInUser
import consus.constants.StringConstants
import consus.resources.PatientResource
import grails.transaction.Transactional
import com.mphrx.chingari.patientLinking.DefaultPatientLinkingUtilityService
import consus.resources.PatientResource
import com.mphrx.util.user.LoggedInUser
import com.mphrx.auth.User
import com.mphrx.util.lang.LanguageUtility

@Transactional
class LabflyDefaultPatientLinkingUtilityService extends DefaultPatientLinkingUtilityService {

    @Override
    public String generateSource(Map sourceCreateCriteria){
        return StringConstants.ASSOCIATED_PATIENT_LINKING;
    }


    @Override
    public Map fetchRelationshipMap(PatientResource patientResource){
        User loggedInUser = new LoggedInUser().getCurrentUser()
        // Determining relationship map based on self patient match
        Map relationshipMapping = new HashMap();
        for(String relationship : relationshipList){
            relationshipMapping.put(relationship, LanguageUtility.getLanguageMapValue("patientLinking.searchForm.selectOptions."+relationship))
        }
        Map relationshipMap = [
                'name'                  : 'relationship',
                'label'                 : LanguageUtility.getLanguageMapValue('patientLinking.searchForm.relationshipLabel'),
                'type'                  : 'select',
                'required'              : true,
                'placeholder'           : LanguageUtility.getLanguageMapValue('patientLinking.searchForm.placeholders.relationship'),
                'requiredError'         : LanguageUtility.getLanguageMapValue('patientLinking.searchForm.relationshipRequiredError'),
                'mappings': relationshipMapping
        ]

        return Collections.unmodifiableMap(relationshipMap)
    }
}