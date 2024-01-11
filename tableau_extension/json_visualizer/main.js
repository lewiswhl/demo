(function(){
var ar_sheet_listener = [];
const markSelection = tableau.TableauEventType.MarkSelectionChanged;

function configure() {
    const popup_url = 'http://localhost/json_visualizer/configuration.html';
    const payload_from_main_to_configuration = ' ';
    const dialog_display_option = { 'height': 800, 'width': 800 };
    tableau.extensions.ui.displayDialogAsync(popup_url, payload_from_main_to_configuration, dialog_display_option)
        .then((payload_from_configuration_to_main) => {
        console.warn('Dialog closed, settings saved!');
        console.log(payload_from_configuration_to_main);
        })
        .catch((error) => {
            switch (error.errorCode) {
                case tableau.ErrorCodes.DialogClosedByUser:
                    console.warn('Dialog was closed by user');
                    break;
                default:
                    console.warn(error.message);
            }
        });
    };

function action_at_SettingsChanged(settingsEvent){
    console.warn('settings changed event');
    var current_setting = tableau.extensions.settings.getAll();
    var sheet_name = current_setting['selected_sheet'];
    var field_name = current_setting['selected_field'];
    console.log('selected sheet is' + sheet_name);
    console.log('selected field is' + current_setting['selected_field']);
    //to unregister previous evenet
    if (ar_sheet_listener.length > 0 && typeof ar_sheet_listener[0] === 'function') {
        ar_sheet_listener[0](); 
        ar_sheet_listener.length = 0; 
      };
    var current_workbook = tableau.extensions.dashboardContent.dashboard.worksheets.find(w => w.name === sheet_name);
    console.log('before event'+ current_workbook.name);
    unregisterEventHandlerFunction_markselection = current_workbook.addEventListener(markSelection, update_json_to_p);
    ar_sheet_listener.push(unregisterEventHandlerFunction_markselection);
    $('#monitoring').text('Tracking ' +'[' + sheet_name + ']' +'.[' + field_name + ']');
    $('#monitoring').css('color', 'green');
    };

function action_at_markselection(){
    var current_setting = tableau.extensions.settings.getAll();
    var worksheet_name = current_setting['selected_sheet'];
    var field_name = current_setting['selected_field'];
    var _direction, _theme;
    try {
      _direction = current_setting['direction'];
      _theme = current_setting['theme'];
    } catch (error) {
      _direction = 'DOWN';
      _theme = 'dark';
    };
    
    function visualize_json_crack (prom_datatable) { //input is the datatable promise
        const column = prom_datatable.columns;
        const target_col_index = prom_datatable.columns.find(i => i.fieldName === field_name).index;
        const json = prom_datatable.data[0][target_col_index].value;
        const jsonCrackEmbed = document.getElementById("jsoncrackEmbed");
        const options = {
        theme: _theme, // "light" or "dark"
        direction: _direction, // "UP", "DOWN", "LEFT", "RIGHT"
        };
    
        jsonCrackEmbed.contentWindow.postMessage({
            json,   //name must be json!!
            options //name must be options!!
            }, "*");
        };
    const target_worksheet = tableau.extensions.dashboardContent.dashboard.worksheets.find(w => w.name === worksheet_name);
    const get_data_option = {ignoreSelection: false, maxRows: 1};
    target_worksheet.getSummaryDataAsync(get_data_option).then(visualize_json_crack); //end getSummaryDataAsync then 
    };

function main(){
    tableau.extensions.initializeAsync({'configure':configure}).then( (abc)=>{
        unregisterEventHandlerFunction = tableau.extensions.settings.addEventListener(tableau.TableauEventType.SettingsChanged, action_at_SettingsChanged);
        var current_setting = tableau.extensions.settings.getAll();
        const _sheet = current_setting['selected_sheet'];
        const _field =  current_setting['selected_field'];
        if (current_setting.hasOwnProperty('selected_sheet') && current_setting.hasOwnProperty('selected_field')) {
            console.log('worksheet and field found without updating the config');
            var target_worksheet_when_initialize = tableau.extensions.dashboardContent.dashboard.worksheets.find(w => w.name === _sheet);
            console.log('WB is ' + target_worksheet_when_initialize.name);
            console.log('Field is ' + _field);
            $('#monitoring').text('Tracking ' +'[' + _sheet + ']' +'.[' + _field + ']');
            $('#monitoring').css('color', 'green');
            unregisterEventHandlerFunction_markselection = target_worksheet_when_initialize.addEventListener(markSelection, action_at_markselection);
            ar_sheet_listener.push(unregisterEventHandlerFunction_markselection);
            };
        });
    };

$(document).ready(main);
})();