import { useEffect, useState } from "react";

import defaultAvatar from "../../assets/default-avatar.svg";

interface AvatarImageProps {
  src: string | null | undefined;
  alt: string;
  className?: string;
  decorative?: boolean;
}

export function AvatarImage({ src, alt, className, decorative = false }: AvatarImageProps) {
  const [failed, setFailed] = useState(false);
  const resolvedSrc = !failed && src ? src : defaultAvatar;

  useEffect(() => {
    setFailed(false);
  }, [src]);

  return (
    <img
      src={resolvedSrc}
      alt={decorative ? "" : alt}
      className={className}
      aria-hidden={decorative ? "true" : undefined}
      onError={() => setFailed(true)}
    />
  );
}
