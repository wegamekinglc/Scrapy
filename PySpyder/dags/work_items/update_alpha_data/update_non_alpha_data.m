function update_non_alpha_data(refDate)
    delete('update_non_alpha_data.log');
    diary('update_non_alpha_data.log')
    diary on
    
    current_folder = pwd;
    project_path = 'D:\dev\svn\lishun\DataUpdate';

    cd(project_path);
    addpath(genpath(project_path));

    try
        updateOneDayNonAlphaData(refDate, refDate);
    catch exception
        disp(exception);
        diary off
        cd(current_folder);
        exit;
    end

    diary off
    cd(current_folder);
    exit
end
