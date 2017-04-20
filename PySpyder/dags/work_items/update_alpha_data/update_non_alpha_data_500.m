function update_non_alpha_data_500(refDate)
    delete('update_non_alpha_data_500.log');
    diary('update_non_alpha_data_500.log')
    diary on
    
    current_folder = pwd;
    project_path = 'D:\dev\svn\lishun\DataUpdate500';

    cd(project_path);
    addpath(genpath(project_path));

    try
        updateOneDayNonAlphaData(refDate, refDate);
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
