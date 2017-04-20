function update_alpha_500(refDate)
    delete('update_alpha_500.log');
    diary('update_alpha_500.log')
    diary on
    
    current_folder = pwd;
    project_path = 'D:\dev\svn\lic\MultiFactorTradingSystem';

    cd(project_path);
    addpath(genpath(project_path));

    try
        updateOneDayAlpha_500(refDate, refDate);
    catch exception
        disp(getReport(exception));
        diary off
        cd(current_folder);
        exit;
    end

    diary off
    cd(current_folder);
    exit
end