import React, { createContext, useState, useContext } from 'react';

const MobileViewContext = createContext();

export const MobileViewProvider = ({ children }) => {
    const [isMobileView, setIsMobileView] = useState(false);

    return (
        <MobileViewContext.Provider value={{ isMobileView, setIsMobileView }}>
            {children}
        </MobileViewContext.Provider>
    );
};

export const useMobileView = () => useContext(MobileViewContext);
